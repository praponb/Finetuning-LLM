WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(amount AS INT) amount_int,
        DATE_FORMAT(paid_at, 'yyyy-MM-dd') paid_at_label,
        COALESCE(method, 'N/A') resolved_method
    FROM payments
    WHERE paid_at >= '2024-04-11'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        paid_at_label,
        resolved_method,
        UPPER(method) method_upper
    FROM valid
    WHERE method IS NOT NULL
      AND amount_int IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT method, id
    FROM latest
    EXCEPT
    SELECT method, id
    FROM payments
    WHERE method IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.method = k.method AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(amount_int) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_int_count_30d,
        CUME_DIST() OVER (ORDER BY amount_int NULLS LAST) amount_int_cume_dist,
        STDDEV_SAMP(amount_int) OVER (PARTITION BY status) status_stddev_samp_amount_int,
        DENSE_RANK() OVER (PARTITION BY method, status ORDER BY amount_int DESC NULLS LAST) amount_int_dense_rank
    FROM reconciled
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        paid_at_label,
        resolved_method,
        method_upper,
        amount_int_count_30d,
        amount_int_cume_dist,
        status_stddev_samp_amount_int,
        amount_int_dense_rank,
        CUME_DIST() OVER w_ordered amount_cume_dist,
        AVG(amount) OVER (PARTITION BY method) method_avg_amount,
        RANK() OVER w_hist amount_int_dense_rank_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY status ORDER BY amount), w_hist AS (PARTITION BY status, method ORDER BY amount_int_dense_rank DESC NULLS FIRST)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount_cume_dist) OVER w_ordered status_max_amount_cume_dist,
        AVG(amount) OVER w_running method_avg_amount_2,
        MAX(amount_int_cume_dist) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_int_cume_dist_max_90d
    FROM enriched_payments
    WINDOW w_ordered AS (PARTITION BY status), w_running AS (PARTITION BY method, status)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_max_amount_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END AS status_max_amount_cume_dist_tier,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    SUM(CASE WHEN status = 'REFUNDED' THEN amount_int_count_30d ELSE 0 END) AS refunded_amount_int_count_30d,
    PERCENTILE_APPROX(amount_int_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS amount_int_cume_dist_quartiles,
    ROUND(SUM(method_avg_amount) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS method_avg_amount_per_success
FROM scored
GROUP BY CASE WHEN status_max_amount_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', paid_at);
