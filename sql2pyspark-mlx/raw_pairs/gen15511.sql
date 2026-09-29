WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE channel IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM cleaned e
    INNER JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        discount_pct_whole,
        promo_id,
        ROW_NUMBER() OVER (PARTITION BY MONTH(starts_on), promo_code ORDER BY promo_id DESC, id, promo_id) AS row_seq,
        AVG(promo_id) OVER (PARTITION BY MONTH(ends_on), promo_code ORDER BY discount_pct_whole, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS promo_id_avg_6
    FROM with_window
    WHERE promotion_promo_code IN ('BUNDLE', 'SPRING10', 'VIP')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_whole NULLS LAST) AS discount_pct_whole_percent_rank,
        LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY channel, promo_code ORDER BY promo_id_avg_6, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        RANK() OVER (PARTITION BY channel ORDER BY row_seq DESC NULLS FIRST) AS row_seq_rank
    FROM scored
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(promo_id_avg_6) OVER (PARTITION BY channel, promo_code ORDER BY last_known_row_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promo_id_avg_6,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY channel), 4) AS row_seq_share,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY promo_code), 4) AS row_seq_share_2
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        discount_pct_whole,
        promo_id,
        promo_id_avg_6,
        discount_pct_whole_percent_rank,
        row_seq_rank,
        row_seq_share,
        row_seq_share_2
    FROM metrics_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_whole_percent_rank DESC NULLS LAST, id, promo_id) = 1
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(row_seq_rank, 0) AS row_seq_rank_filled
    FROM ranked
    WHERE promo_id_avg_6 BETWEEN 4 AND 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        channel,
        discount_pct_whole_percent_rank,
        row_seq_share_2
    FROM cleaned_promotions
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (discount_pct_whole_percent_rank, row_seq_share_2)
);
