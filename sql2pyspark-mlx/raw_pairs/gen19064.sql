WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) AS row_num
    FROM promotions
    QUALIFY row_num = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 10)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct DESC) AS discount_pct_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY channel, promotion_promo_code ORDER BY starts_on, id, promo_id) AS row_seq,
        SUM(promo_id) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promo_id
    FROM with_window
    WHERE starts_on >= '2024-02-10'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_windows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promotion_discount_pct_2,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id, promo_id, promo_id_2) AS seq_no,
        AVG(promotion_discount_pct_2) OVER (PARTITION BY promotion_promo_code ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS promotion_discount_pct_2_avg_3_prior,
        SUM(discount_pct_percent_rank) OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY ends_on DESC, id, promo_id, promo_id_2) AS event_seq,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY promo_id DESC NULLS LAST) AS promo_id_cume_dist
    FROM matched
)
SELECT
    id,
    promo_code,
    ends_on,
    promo_id,
    promotion_discount_pct,
    promo_id_2,
    promotion_discount_pct_2,
    seq_no,
    promotion_discount_pct_2_avg_3_prior,
    CASE WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS promo_code_zone
FROM metrics
