WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(discount_pct, 2) AS discount_pct_rounded,
        DATE_ADD(ends_on, 17) AS ends_on_plus_17d
    FROM promotions
    WHERE ends_on >= DATE '2023-08-22'
      AND MONTH(starts_on) IN (1, 6, 8)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY discount_pct_rounded DESC, id) AS discount_pct_rounded_bucket,
        CUME_DIST() OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY discount_pct_rounded DESC NULLS FIRST) AS discount_pct_rounded_cume_dist,
        FIRST_VALUE(discount_pct_rounded) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct_rounded,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS row_seq
    FROM filtered
    WHERE promo_code = 'VIP'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct_rounded,
        ends_on_plus_17d,
        first_known_discount_pct_rounded,
        row_seq,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key
    FROM with_windows
    WHERE ends_on BETWEEN '2023-04-01' AND '2025-08-26'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq, id) AS row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_rounded, id) AS row_seq_3
    FROM cleaned
),
scored_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        discount_pct_rounded,
        first_known_discount_pct_rounded,
        promo_code_channel_key,
        SUM(first_known_discount_pct_rounded) OVER w_grp AS last4_sum_first_known_discount_pct_rounded,
        AVG(first_known_discount_pct_rounded) OVER w_grp AS last4_avg_first_known_discount_pct_rounded,
        MAX(first_known_discount_pct_rounded) OVER w_grp AS last4_max_first_known_discount_pct_rounded
    FROM scored
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    discount_pct_rounded,
    first_known_discount_pct_rounded,
    promo_code_channel_key
FROM scored_promotions
