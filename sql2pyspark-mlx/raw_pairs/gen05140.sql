WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) AS rank_in_group
    FROM promotions
    QUALIFY rank_in_group = 1
),
attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest s
    JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        rank_in_group,
        promo_id,
        promotion_promo_code,
        LAG(promotion_discount_pct) OVER (PARTITION BY channel, promotion_promo_code ORDER BY ends_on, id, promo_id) AS prev_promotion_discount_pct,
        LAG(promo_id) OVER (PARTITION BY channel, promotion_promo_code ORDER BY ends_on, id, promo_id) AS prev_promo_id,
        ROW_NUMBER() OVER (PARTITION BY channel, promotion_promo_code ORDER BY ends_on, id, promo_id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY MONTH(ends_on), channel ORDER BY discount_pct DESC) AS discount_pct_cume_dist
    FROM attributed
),
filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        rank_in_group,
        promo_id,
        promotion_promo_code,
        prev_promotion_discount_pct,
        prev_promo_id,
        seq_no,
        discount_pct_cume_dist,
        CEIL(prev_promo_id) AS prev_promo_id_whole,
        COALESCE(promotion_promo_code, 'UNKNOWN') AS promotion_promo_code_filled
    FROM enriched
    WHERE starts_on > DATE '2023-09-14'
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        rank_in_group,
        promo_id,
        prev_promotion_discount_pct,
        seq_no,
        discount_pct_cume_dist,
        promotion_promo_code_filled,
        LAG(prev_promotion_discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY seq_no, id, promo_id) AS prev_prev_promotion_discount_pct,
        LEAD(prev_promotion_discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY seq_no, id, promo_id) AS next_prev_promotion_discount_pct,
        LEAD(discount_pct_cume_dist) OVER (PARTITION BY promo_code, channel ORDER BY seq_no, id, promo_id) AS next_discount_pct_cume_dist,
        LAG(discount_pct_cume_dist) OVER (PARTITION BY promo_code, channel ORDER BY seq_no, id, promo_id) AS prev_discount_pct_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY seq_no, id, promo_id) AS row_seq
    FROM filtered
    WHERE YEAR(starts_on) >= 2025
)
SELECT
    promo_id,
    CASE WHEN discount_pct_cume_dist >= 0.77 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_cume_dist_tier,
    seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY GROUPING SETS ((promo_id, CASE WHEN discount_pct_cume_dist >= 0.77 THEN 'HIGH' ELSE 'LOW' END, seq_no), (promo_id, CASE WHEN discount_pct_cume_dist >= 0.77 THEN 'HIGH' ELSE 'LOW' END), (promo_id), ())
