WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ROUND(DATEDIFF(starts_on, ends_on) / 30, 1) AS months_ends_on_to_starts_on
    FROM promotions
    WHERE starts_on < '2025-03-23'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '30' DAY
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        months_ends_on_to_starts_on,
        promo_id,
        COUNT(discount_pct) OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_pct,
        NTILE(5) OVER (PARTITION BY channel ORDER BY promo_id DESC, id, promo_id) AS promo_id_bucket
    FROM with_window
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_min_discount_pct NULLS FIRST, id, promo_id) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_id,
        running_min_discount_pct,
        AVG(running_min_discount_pct) OVER w_part AS running_min_discount_pct_avg_6_prior,
        LAST_VALUE(running_count_discount_pct) IGNORE NULLS OVER (PARTITION BY channel ORDER BY months_ends_on_to_starts_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_count_discount_pct
    FROM latest
    WINDOW w_part AS (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    promo_id,
    running_min_discount_pct,
    running_min_discount_pct_avg_6_prior,
    last_known_running_count_discount_pct
FROM metrics
ORDER BY id
