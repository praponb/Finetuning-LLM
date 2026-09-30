WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
matched_keys AS (
    -- Set difference on keys
    SELECT service, id
    FROM lines
    EXCEPT DISTINCT
    SELECT service, id
    FROM app_logs
    WHERE log_ts >= '2025-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN matched_keys k ON s.service = k.service AND s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN service = 'billing' THEN 1 ELSE 0 END AS is_billing,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM kept_rows
    WHERE log_ts >= TIMESTAMP '2025-06-01 13:00:00'
      AND attr_key IS DISTINCT FROM 'region'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        host,
        attr_key,
        value,
        is_billing,
        attr_key_filled,
        SUM(is_billing) OVER w_grp AS is_billing_sum_2h,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY is_billing) AS is_billing_percent_rank
    FROM valid
    WINDOW w_grp AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        attr_key,
        value,
        attr_key_filled,
        is_billing_percent_rank,
        is_billing_sum_2h / NULLIF(is_billing_percent_rank, 0) AS is_billing_sum_2h_per_is_billing_percent_rank
    FROM enriched
    WHERE is_billing <> 72
),
metrics AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY attr_key ORDER BY is_billing_sum_2h_per_is_billing_percent_rank DESC NULLS LAST) AS is_billing_sum_2h_per_is_billing_percent_rank_cume_dist
    FROM cleaned
    WHERE is_billing_sum_2h_per_is_billing_percent_rank IS NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_billing_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END AS is_billing_percent_rank_tier,
        attr_key,
        COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
        ROUND(AVG(is_billing_sum_2h_per_is_billing_percent_rank), 2) AS avg_is_billing_sum_2h_per_is_billing_percent_rank,
        PERCENTILE_APPROX(is_billing_sum_2h_per_is_billing_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS is_billing_sum_2h_per_is_billing_percent_rank_quartiles,
        GROUPING_ID() AS grouping_level
    FROM metrics
    GROUP BY ROLLUP (CASE WHEN is_billing_percent_rank >= 0.78 THEN 'LARGE' ELSE 'SMALL' END, attr_key)
    HAVING COUNT(*) FILTER (WHERE attr_key = 'trace') >= 1
)
SELECT
    attr_key,
    avg_is_billing_sum_2h_per_is_billing_percent_rank,
    is_billing_sum_2h_per_is_billing_percent_rank_quartiles,
    grouping_level
FROM stats
ORDER BY attr_key NULLS FIRST;
