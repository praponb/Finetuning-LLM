WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        LENGTH(channel) AS channel_length,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on
    FROM promotions
    WHERE starts_on > DATE '2023-07-23'
      AND starts_on <= '2025-07-05'
),
enriched AS (
    SELECT
        *,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY days_since_ends_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY days_since_ends_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promo_code ORDER BY days_since_ends_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_discount_pct,
        STDDEV_SAMP(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_stddev_samp_discount_pct
    FROM filtered
),
with_windows AS (
    SELECT
        *,
        COUNT(last3_avg_discount_pct) OVER w_recent AS running_count_last3_avg_discount_pct,
        MAX(last3_avg_discount_pct) OVER w_recent AS running_max_last3_avg_discount_pct,
        SUM(last3_avg_discount_pct) OVER w_recent AS running_sum_last3_avg_discount_pct
    FROM enriched
    WINDOW w_recent AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        last3_avg_discount_pct,
        last3_count_discount_pct,
        promo_code_stddev_samp_discount_pct,
        running_count_last3_avg_discount_pct,
        row_num,
        FLOOR(running_sum_last3_avg_discount_pct) AS running_sum_last3_avg_discount_pct_whole,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM deduped
    WHERE starts_on > DATE '2023-09-02'
)
SELECT
    CASE WHEN running_count_last3_avg_discount_pct >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_last3_avg_discount_pct_tier,
    CASE WHEN promo_code_stddev_samp_discount_pct >= 0 THEN 'HIGH' ELSE 'LOW' END AS promo_code_stddev_samp_discount_pct_tier,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    SUM(CASE WHEN channel = 'mobile' THEN running_sum_last3_avg_discount_pct_whole ELSE 0 END) AS mobile_running_sum_last3_avg_discount_pct_whole,
    ROUND(AVG(running_count_last3_avg_discount_pct), 2) AS avg_running_count_last3_avg_discount_pct,
    APPROX_PERCENTILE(running_sum_last3_avg_discount_pct_whole, ARRAY(0.25, 0.5, 0.75)) AS running_sum_last3_avg_discount_pct_whole_quartiles,
    RANK() OVER (PARTITION BY CASE WHEN running_count_last3_avg_discount_pct >= 13 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN channel = 'mobile' THEN running_sum_last3_avg_discount_pct_whole ELSE 0 END) DESC) AS mobile_running_sum_last3_avg_discount_pct_whole_rank
FROM valid
GROUP BY CASE WHEN running_count_last3_avg_discount_pct >= 13 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN promo_code_stddev_samp_discount_pct >= 0 THEN 'HIGH' ELSE 'LOW' END
