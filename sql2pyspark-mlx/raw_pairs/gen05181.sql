WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 7)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        starts_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promotion_promo_code ORDER BY discount_pct DESC, id, promo_id) AS dedup_rank
        FROM attributed
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id, id, promo_id) AS event_seq,
        SUM(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promotion_discount_pct
    FROM deduped
    WHERE promo_id > 4
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY event_seq DESC, id, promo_id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promotion_promo_code ORDER BY starts_on DESC) AS row_num_2
        FROM latest
    ) x
    WHERE row_num_2 = 1
)
-- Multi-level aggregation
SELECT
    promo_id,
    COUNT_IF(promotion_promo_code = 'BUNDLE') AS bundle_events,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'SPRING10') AS spring10_rows,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'WELCOME5') AS welcome5_rows,
    SUM(discount_pct) FILTER (WHERE promotion_promo_code = 'VIP') AS vip_total_discount_pct
FROM latest_promotions
GROUP BY 1
ORDER BY bundle_events NULLS FIRST;
