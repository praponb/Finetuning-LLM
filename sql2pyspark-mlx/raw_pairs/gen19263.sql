WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        starts_on,
        discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id ASC) AS rank_in_group
        FROM promotions
    ) ranked
    WHERE rank_in_group = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        starts_on,
        discount_pct,
        promo_id,
        promo_code,
        COUNT(promotion_discount_pct) OVER w_recent AS promo_code_count_promotion_discount_pct,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM matched
    WINDOW w_recent AS (PARTITION BY promo_code), w_hist AS (ORDER BY promotion_discount_pct ASC, id ASC, promo_id ASC)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq DESC, id ASC, promo_id ASC) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    promo_code,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END), 0), 2) AS discount_pct_per_vip
FROM deduped
GROUP BY 1
HAVING ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END), 0), 2) > 0;
