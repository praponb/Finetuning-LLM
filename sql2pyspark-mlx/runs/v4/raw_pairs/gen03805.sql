WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-10-24')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        days_starts_on_to_ends_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        AVG(discount_pct) OVER (ORDER BY discount_pct, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY ends_on, id, promo_id) AS seq_no
    FROM attributed
),
filtered_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN promo_code IN ('BUNDLE', 'SPRING10') THEN 'Priority' ELSE 'Other' END AS promo_code_group,
        COALESCE(promotion_promo_code, 'NONE') AS promotion_promo_code_clean
    FROM metrics
    WHERE channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        days_starts_on_to_ends_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct_avg_3_prior,
        seq_no,
        promo_code_group,
        promotion_promo_code_clean,
        RANK() OVER (PARTITION BY promo_code ORDER BY days_starts_on_to_ends_on DESC) AS days_starts_on_to_ends_on_rank,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct_avg_3_prior, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code), 4) AS promotion_discount_pct_share
    FROM filtered_promotions
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY promotion_promo_code ORDER BY starts_on) = 1
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct_avg_3_prior DESC, id, promo_id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
)
-- Running and moving aggregates
SELECT
    *,
    NTILE(5) OVER (PARTITION BY promo_code ORDER BY days_starts_on_to_ends_on_rank, id, promo_id) AS days_starts_on_to_ends_on_rank_bucket,
    DENSE_RANK() OVER w_part AS discount_pct_avg_3_prior_dense_rank,
    STDDEV_SAMP(days_starts_on_to_ends_on_rank) OVER (PARTITION BY promotion_promo_code) AS promotion_promo_code_stddev_samp_days_starts_on_to_ends_on_rank,
    MIN(promotion_discount_pct) OVER w_running AS promo_code_min_promotion_discount_pct
FROM deduped_promotions
WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct_avg_3_prior DESC NULLS LAST), w_running AS (PARTITION BY promo_code)
