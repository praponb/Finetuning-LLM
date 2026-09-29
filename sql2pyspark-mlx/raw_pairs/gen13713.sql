WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value,
        SUM(is_new_burst) OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id ASC, k ASC) AS row_seq,
        NTILE(5) OVER (PARTITION BY k ORDER BY burst_id ASC, id ASC, k ASC) AS burst_id_bucket,
        ROW_NUMBER() OVER (ORDER BY burst_id DESC, id ASC, k ASC) AS event_seq
    FROM islands
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(burst_id) OVER w_hist AS prev_burst_id,
        LEAD(burst_id) OVER w_hist AS next_burst_id,
        LAG(row_seq) OVER w_hist AS prev_row_seq,
        LEAD(row_seq) OVER w_hist AS next_row_seq,
        ROW_NUMBER() OVER w_hist AS row_seq_2
    FROM scored
    WHERE row_seq BETWEEN 5 AND 7
    WINDOW w_hist AS (PARTITION BY k ORDER BY row_seq ASC, id ASC, k ASC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN burst_id_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_bucket_tier,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    PERCENTILE_APPROX(burst_id_bucket, 0.9) AS burst_id_bucket_p90,
    COUNT(CASE WHEN k = 'weight_kg' THEN 1 END) AS weight_kg_count,
    APPROX_PERCENTILE(row_seq_2, 0.95) AS row_seq_2_p95
FROM enriched
GROUP BY 1
HAVING APPROX_PERCENTILE(row_seq_2, 0.95) > 0;
