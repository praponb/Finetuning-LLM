WITH latest AS (
    -- QUALIFY-style window filter
    SELECT id, discount_pct
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        discount_pct,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2,
        LAG(discount_pct, 1, 0) OVER (ORDER BY discount_pct, id) AS prev_discount_pct
    FROM latest
    WHERE discount_pct <= 0.23
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN discount_pct >= 0.33 THEN 'High' WHEN discount_pct >= 0.1 THEN 'Medium' ELSE 'Low' END AS discount_pct_band
    FROM enriched
    WHERE prev_discount_pct < 0.3
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        discount_pct,
        discount_pct_band,
        LAG(running_discount_pct_2, 1) OVER (ORDER BY running_discount_pct, id) AS prev_running_discount_pct_2,
        RANK() OVER (ORDER BY running_discount_pct_2 DESC NULLS LAST) AS running_discount_pct_2_rank
    FROM cleaned
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_running_discount_pct_2 >= 2 THEN 'HIGH' ELSE 'LOW' END AS prev_running_discount_pct_2_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        AVG(prev_running_discount_pct_2) AS avg_prev_running_discount_pct_2,
        APPROX_PERCENTILE(running_discount_pct_2_rank, 0.5) AS running_discount_pct_2_rank_p50,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids_2,
        ROUND(AVG(discount_pct), 2) AS avg_discount_pct
    FROM scored
    GROUP BY prev_running_discount_pct_2_tier
)
SELECT
    prev_running_discount_pct_2_tier,
    distinct_ids,
    avg_prev_running_discount_pct_2,
    event_count,
    distinct_ids_2,
    avg_discount_pct
FROM daily
ORDER BY distinct_ids DESC
