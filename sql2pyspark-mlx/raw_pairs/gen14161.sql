WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        ADD_MONTHS(ends_on, 12) AS ends_on_plus_months,
        LOWER(channel) AS channel_lower
    FROM promotions
    WHERE starts_on > TO_DATE('2023-07-11')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        ends_on_plus_months,
        channel_lower
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
valid_promotions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        ends_on,
        discount_pct,
        channel_lower,
        DATEDIFF(ends_on_plus_months, ends_on) / 7 AS weeks_ends_on_to_ends_on_plus_months
    FROM latest
    WHERE discount_pct BETWEEN 0.15 AND 0.21
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid_promotions src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '7' DAY
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched a
    INNER JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id, promo_id, promo_id_2) AS row_num
    FROM attributed
    QUALIFY row_num = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_3
    FROM ranked src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Multi-level aggregation
SELECT
    CASE WHEN promotion_discount_pct_3 >= 0.27 THEN 'PREMIUM' ELSE 'STANDARD' END AS promotion_discount_pct_3_tier,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
    MAX(promotion_discount_pct_2) FILTER (WHERE promotion_promo_code = 'VIP') AS vip_max_promotion_discount_pct_2,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    COUNT(CASE WHEN promotion_promo_code_2 = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_window
GROUP BY promotion_discount_pct_3_tier
