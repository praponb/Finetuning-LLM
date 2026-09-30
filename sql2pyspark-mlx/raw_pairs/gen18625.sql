WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(promo_code) AS clean_promo_code,
        ADD_MONTHS(ends_on, 12) AS ends_on_plus_months,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on BETWEEN '2024-02-01' AND '2025-01-01'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on
    FROM base
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY discount_pct) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM deduped e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 7)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct) OVER (ORDER BY ends_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY rank_in_group, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior
    FROM matched
    WHERE starts_on <= '2024-01-01'
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    promo_id,
    discount_pct,
    discount_pct_avg_2_prior,
    discount_pct_avg_5_prior,
    CASE WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS promo_code_zone
FROM enriched
ORDER BY promo_code DESC, promo_id
