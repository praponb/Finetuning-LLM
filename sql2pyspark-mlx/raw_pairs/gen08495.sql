WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC) AS row_seq,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC) AS next_discount_pct
    FROM promotions
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        next_discount_pct,
        MIN(next_discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_discount_pct,
        SUM(next_discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_discount_pct,
        AVG(next_discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_discount_pct,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY channel ORDER BY next_discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY running_avg_next_discount_pct DESC NULLS FIRST, id ASC) AS latest_rank
    FROM enriched_promotions
    QUALIFY latest_rank = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM ranked a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 3)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct
    FROM matched e
    INNER JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 14 DAYS
)
SELECT
    id,
    next_discount_pct,
    running_avg_next_discount_pct,
    first_known_row_seq,
    latest_rank,
    promo_id,
    promotion_promo_code,
    promo_id_2,
    promotion_discount_pct
FROM attributed
ORDER BY promotion_promo_code ASC
