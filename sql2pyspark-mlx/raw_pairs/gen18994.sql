WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_discount_pct,
        NTILE(3) OVER (PARTITION BY channel ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM promotions
    WHERE starts_on <= '2023-03-07'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM metrics src
    INNER JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '7' DAY
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct DESC, id, promo_id, promo_id_2) AS dedup_rank
    FROM matched_promotions
    QUALIFY dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(last3_min_discount_pct, 0) AS last3_min_discount_pct_filled
    FROM deduped
    WHERE ends_on >= TO_DATE('2023-06-03')
      AND promotion_promo_code IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM cleaned src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '30' DAY
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    last3_min_discount_pct,
    last3_sum_discount_pct,
    discount_pct_bucket,
    CASE WHEN promo_code RLIKE '^(WELCOME5|VIP)$' THEN 'GROUP_B' WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS promo_code_zone
FROM with_window
ORDER BY ends_on DESC NULLS LAST, discount_pct;
