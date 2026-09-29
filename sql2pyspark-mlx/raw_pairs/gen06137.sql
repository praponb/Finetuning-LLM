WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
matched_keys AS (
    -- Set intersection on keys
    SELECT currency
    FROM lines
    INTERSECT
    SELECT currency
    FROM price_tiers
    WHERE currency <> 'USD'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN matched_keys k ON s.currency = k.currency
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        val,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
        COUNT(val) OVER (PARTITION BY currency) AS currency_count_val
    FROM verified
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        val,
        currency_count_val,
        RANK() OVER (ORDER BY currency_count_val DESC) AS currency_count_val_rank,
        PERCENT_RANK() OVER (ORDER BY currency_count_val DESC) AS currency_count_val_percent_rank,
        DENSE_RANK() OVER (ORDER BY currency_count_val DESC NULLS FIRST) AS currency_count_val_dense_rank,
        CUME_DIST() OVER (ORDER BY currency_count_val ASC) AS currency_count_val_cume_dist
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN currency_count_val_cume_dist >= 0.64 THEN 'LARGE' ELSE 'SMALL' END AS currency_count_val_cume_dist_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(currency_count_val_percent_rank, 0.9) AS currency_count_val_percent_rank_p90,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(val, 0.95) AS val_p95,
    MIN(currency_count_val_dense_rank) AS min_currency_count_val_dense_rank
FROM metrics
GROUP BY ROLLUP (CASE WHEN currency_count_val_cume_dist >= 0.64 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY currency_count_val_cume_dist_tier ASC
