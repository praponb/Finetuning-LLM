WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(promo_code) clean_promo_code
    FROM promotions
    WHERE promo_code = 'WELCOME5'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        clean_promo_code,
        PERCENT_RANK() OVER w_recent discount_pct_percent_rank,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct,
        LAG(discount_pct, 2, 0) OVER (PARTITION BY promo_code, channel ORDER BY starts_on ASC, id ASC) prev_discount_pct
    FROM filtered
    WINDOW w_recent AS (ORDER BY discount_pct DESC)
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        clean_promo_code,
        discount_pct_percent_rank,
        running_discount_pct,
        cumulative_discount_pct,
        prev_discount_pct,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY channel, promo_code ORDER BY running_discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel,
        LEAD(prev_discount_pct, 1) OVER (PARTITION BY channel, promo_code ORDER BY cumulative_discount_pct ASC, id ASC) next_prev_discount_pct,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel_2
    FROM with_windows
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows_promotions s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 3)
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        channel,
        SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
        COUNT(DISTINCT promotion_promo_code) AS distinct_promotion_promo_codes,
        SUM(CASE WHEN promo_code = 'SPRING10' THEN prev_discount_pct ELSE 0 END) AS spring10_prev_discount_pct,
        APPROX_PERCENTILE(cumulative_discount_pct, 0.95) AS cumulative_discount_pct_p95,
        COUNT(*) AS num_rows
    FROM with_window
    GROUP BY DATE_TRUNC('MONTH', ends_on), channel
    HAVING distinct_promotion_promo_codes < 1
)
SELECT
    activity_month,
    channel,
    promotion_promo_code_list,
    spring10_prev_discount_pct,
    cumulative_discount_pct_p95,
    num_rows,
    CASE WHEN channel RLIKE '^(web|partner)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS channel_zone
FROM daily
ORDER BY cumulative_discount_pct_p95 DESC NULLS FIRST;
