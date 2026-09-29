WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-08-04')
      AND starts_on <= '2025-10-21'
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY promotion_discount_pct DESC NULLS LAST) AS promotion_discount_pct_percent_rank,
        SUM(discount_pct) OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        AVG(promo_id) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS promo_id_avg_5_prior
    FROM matched
),
filtered AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        channel_filled,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promotion_discount_pct_percent_rank,
        running_discount_pct,
        promo_id_avg_5_prior,
        promotion_discount_pct + promo_id_avg_5_prior AS promotion_discount_pct_plus_promo_id_avg_5_prior
    FROM scored
    WHERE ends_on > DATE '2023-10-10'
),
filtered_promotions AS (
    SELECT
        *,
        promotion_discount_pct_percent_rank - promotion_discount_pct_plus_promo_id_avg_5_prior AS promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior,
        FLOOR(promotion_discount_pct) AS promotion_discount_pct_whole
    FROM filtered
    WHERE promo_id >= 5
),
enriched AS (
    SELECT
        id,
        channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promotion_discount_pct_percent_rank,
        promo_id_avg_5_prior,
        promotion_discount_pct_plus_promo_id_avg_5_prior,
        promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior,
        NTILE(4) OVER (PARTITION BY channel ORDER BY promotion_discount_pct DESC, id, promo_id) AS promotion_discount_pct_bucket,
        AVG(running_discount_pct) OVER (PARTITION BY channel, promotion_promo_code ORDER BY promo_id_avg_5_prior, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_discount_pct_avg_6,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY promotion_discount_pct_percent_rank NULLS FIRST) AS promotion_discount_pct_percent_rank_cume_dist
    FROM filtered_promotions
),
cleaned AS (
    SELECT
        id,
        channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct_percent_rank,
        promotion_discount_pct_plus_promo_id_avg_5_prior,
        promotion_discount_pct_percent_rank_minus_promotion_discount_pct_plus_promo_id_avg_5_prior,
        running_discount_pct_avg_6,
        promotion_discount_pct_percent_rank_cume_dist,
        CASE WHEN promo_id >= 10 THEN 'High' WHEN promo_id >= 5 THEN 'Medium' ELSE 'Low' END AS promo_id_band,
        COALESCE(promotion_promo_code, 'NONE') AS promotion_promo_code_clean
    FROM enriched
    WHERE channel IS NOT NULL
)
SELECT *
FROM (
    SELECT
        promotion_promo_code,
        channel,
        promo_id
    FROM cleaned
) src
PIVOT (
    MAX(promo_id) AS total,
    COUNT(1) AS item_count
    FOR channel IN ('web', 'store', 'partner')
)
