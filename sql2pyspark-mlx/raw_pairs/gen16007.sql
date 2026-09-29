WITH metrics AS (
    SELECT
        id,
        channel,
        ends_on,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        NTILE(3) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        MIN(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_min_discount_pct
    FROM promotions
    WHERE YEAR(starts_on) >= 2025
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 3)
),
matched AS (
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed e
    JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '7' DAY
),
latest AS (
    SELECT
        id,
        channel,
        ends_on,
        last_known_channel,
        promo_code_min_discount_pct,
        promo_id,
        promo_code,
        discount_pct,
        promo_id_2,
        promotion_discount_pct
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY promo_id DESC) AS dedup_rank
        FROM matched
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    channel,
    ends_on,
    last_known_channel,
    promo_code_min_discount_pct,
    promo_id,
    promo_code,
    promo_id_2,
    NTILE(10) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id, promo_id, promo_id_2) AS discount_pct_bucket,
    SUM(promo_id_2) OVER (PARTITION BY channel ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promo_id_2,
    LAG(promo_id_2, 1) OVER (PARTITION BY YEAR(ends_on), promo_code ORDER BY promotion_discount_pct, id, promo_id, promo_id_2) AS prev_promo_id_2,
    LAST_VALUE(promo_id_2) IGNORE NULLS OVER (ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promo_id_2
FROM latest
