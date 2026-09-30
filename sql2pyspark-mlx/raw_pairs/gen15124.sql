WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATEDIFF(ends_on, starts_on) / 7 AS weeks_starts_on_to_ends_on,
        LAST_DAY(starts_on) AS starts_on_month_end
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'partner'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct) OVER w_hist AS channel_avg_discount_pct,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY weeks_starts_on_to_ends_on DESC NULLS FIRST) AS weeks_starts_on_to_ends_on_dense_rank,
        LEAD(weeks_starts_on_to_ends_on, 2) OVER (PARTITION BY MONTH(ends_on), channel ORDER BY discount_pct, id) AS next_weeks_starts_on_to_ends_on,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY weeks_starts_on_to_ends_on, id) AS row_seq,
        CUME_DIST() OVER w_running AS discount_pct_cume_dist
    FROM base
    WHERE starts_on > '2025-07-01'
    WINDOW w_hist AS (PARTITION BY channel), w_running AS (PARTITION BY promo_code ORDER BY discount_pct DESC)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct,
        RANK() OVER (PARTITION BY promo_code ORDER BY weeks_starts_on_to_ends_on NULLS LAST) AS weeks_starts_on_to_ends_on_rank
    FROM with_windows
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 7)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    starts_on_month_end,
    channel_avg_discount_pct,
    next_weeks_starts_on_to_ends_on,
    weeks_starts_on_to_ends_on_rank,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS promo_code_zone
FROM attributed
ORDER BY starts_on DESC NULLS FIRST
