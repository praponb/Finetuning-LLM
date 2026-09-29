WITH matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions src
    INNER JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct) AS rank_in_group
        FROM matched
    ) t
    WHERE rank_in_group = 1
),
deduped_promotions AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct DESC, id, promo_id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
scored AS (
    SELECT
        id,
        ends_on,
        discount_pct,
        promo_id,
        MIN(promo_id) OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_promo_id,
        SUM(promo_id) OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_promo_id,
        AVG(promo_id) OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_promo_id,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY channel), 4) AS promo_id_share
    FROM deduped_promotions
)
SELECT
    promo_id,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    COUNT(*) AS row_count
FROM scored
GROUP BY promo_id
ORDER BY row_count DESC NULLS FIRST;
