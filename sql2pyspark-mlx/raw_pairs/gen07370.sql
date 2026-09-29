WITH metrics AS (
    SELECT
        *,
        MAX(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct,
        LAG(discount_pct, 1) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS prev_discount_pct
    FROM promotions
),
cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        running_max_discount_pct,
        running_sum_discount_pct,
        running_min_discount_pct,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on,
        running_max_discount_pct - discount_pct AS running_max_discount_pct_minus_discount_pct,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM metrics
    WHERE starts_on > '2023-11-19'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY running_sum_discount_pct DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
filtered AS (
    SELECT *, UPPER(channel) AS channel_upper
    FROM ranked
    WHERE starts_on >= TO_DATE('2023-09-04')
      AND channel IS NOT NULL
),
metrics_promotions AS (
    SELECT
        id,
        starts_on,
        discount_pct,
        running_sum_discount_pct,
        running_min_discount_pct,
        days_ends_on_to_starts_on,
        running_max_discount_pct_minus_discount_pct,
        latest_rank,
        channel_upper,
        AVG(running_max_discount_pct_minus_discount_pct) OVER (PARTITION BY channel ORDER BY running_min_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_max_discount_pct_minus_discount_pct,
        COUNT(running_max_discount_pct_minus_discount_pct) OVER (PARTITION BY channel ORDER BY running_min_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_max_discount_pct_minus_discount_pct,
        MAX(running_max_discount_pct_minus_discount_pct) OVER (PARTITION BY channel ORDER BY running_min_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_max_discount_pct_minus_discount_pct,
        LAST_VALUE(running_max_discount_pct, TRUE) OVER (PARTITION BY channel ORDER BY running_max_discount_pct_minus_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_max_discount_pct
    FROM filtered
),
enriched AS (
    SELECT
        id,
        running_min_discount_pct,
        latest_rank,
        channel_upper,
        running_avg_running_max_discount_pct_minus_discount_pct,
        running_count_running_max_discount_pct_minus_discount_pct,
        last_known_running_max_discount_pct,
        CUME_DIST() OVER w_part AS running_min_discount_pct_cume_dist,
        LAST_VALUE(running_count_running_max_discount_pct_minus_discount_pct, TRUE) OVER w_recent AS last_known_running_count_running_max_discount_pct_minus_discount_pct
    FROM metrics_promotions
    WINDOW w_part AS (PARTITION BY YEAR(starts_on) ORDER BY running_min_discount_pct), w_recent AS (ORDER BY running_min_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_promotions AS (
    SELECT *, running_count_running_max_discount_pct_minus_discount_pct - latest_rank AS running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank
    FROM enriched
    WHERE running_min_discount_pct_cume_dist < 0.62
)
SELECT
    CASE WHEN last_known_running_max_discount_pct >= 0.33 THEN 'HIGH' ELSE 'LOW' END AS last_known_running_max_discount_pct_tier,
    CASE WHEN running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_count_running_max_discount_pct_minus_discount_pct_minus_latest_rank_tier,
    ROUND(AVG(running_avg_running_max_discount_pct_minus_discount_pct), 2) AS avg_running_avg_running_max_discount_pct_minus_discount_pct,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(last_known_running_max_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS last_known_running_max_discount_pct_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered_promotions
GROUP BY 1, 2
ORDER BY avg_running_avg_running_max_discount_pct_minus_discount_pct DESC NULLS LAST
