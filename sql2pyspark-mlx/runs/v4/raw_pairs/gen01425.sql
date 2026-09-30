WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(starts_on, 'yyyy-MM') starts_on_label,
        COALESCE(channel, 'DIRECT') resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on < '2023-10-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY discount_pct DESC) discount_pct_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) event_seq
    FROM valid
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_percent_rank DESC NULLS LAST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on DESC, id) = 1
),
latest_promotions_2 AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS dedup_rank
        FROM latest_promotions
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id) AS row_num
        FROM latest_promotions_2
    ) ranked
    WHERE row_num = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        channel,
        discount_pct_percent_rank,
        promotion_discount_pct
    FROM matched
) src
UNPIVOT (
    value FOR kpi IN (discount_pct_percent_rank, promotion_discount_pct)
);
