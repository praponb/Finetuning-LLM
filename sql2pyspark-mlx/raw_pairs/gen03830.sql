WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), starts_on) / 7 AS weeks_since_starts_on
    FROM promotions
    WHERE ends_on >= DATE '2023-11-07'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid e
    JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id, promo_id) AS rn
        FROM attributed
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(weeks_since_starts_on) OVER w_hist AS next_weeks_since_starts_on,
        LAG(weeks_since_starts_on) OVER w_hist AS prev_weeks_since_starts_on,
        LEAD(promo_id) OVER w_hist AS next_promo_id,
        LAG(promo_id) OVER w_hist AS prev_promo_id,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM ranked
    WINDOW w_hist AS (PARTITION BY channel, promotion_promo_code ORDER BY weeks_since_starts_on, id, promo_id)
)
-- Grouped metrics
SELECT
    promo_id,
    channel,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count,
    SUM(promo_id) FILTER (WHERE channel = 'mobile') AS mobile_total_promo_id,
    SUM(CASE WHEN promo_code = 'SPRING10' THEN promo_id ELSE 0 END) AS spring10_promo_id,
    GROUPING(promo_id) AS grouping_level
FROM with_windows
GROUP BY CUBE (promo_id, channel)
ORDER BY channel DESC;
