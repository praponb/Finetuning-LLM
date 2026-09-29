WITH matched_keys AS (
    SELECT promo_code
    FROM promotions
    INTERSECT ALL
    SELECT promo_code
    FROM promotions
    WHERE starts_on BETWEEN '2023-08-23' AND '2024-08-31'
),
kept_rows AS (
    SELECT s.*
    FROM promotions s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code
),
metrics AS (
    SELECT
        id,
        starts_on,
        ends_on,
        RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct) AS discount_pct_rank,
        COUNT(discount_pct) OVER (PARTITION BY DATE_TRUNC('WEEK', starts_on)) AS starts_on_period_count_discount_pct
    FROM kept_rows
    WHERE YEAR(starts_on) >= 2025
),
matched_keys_2 AS (
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM promotions
    WHERE discount_pct <= 0.28
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys_2 k ON s.id = k.id
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY starts_on_period_count_discount_pct DESC NULLS FIRST) AS starts_on_period_count_discount_pct_percent_rank,
        DENSE_RANK() OVER (ORDER BY discount_pct_rank NULLS FIRST) AS discount_pct_rank_dense_rank
    FROM verified
),
enriched_promotions AS (
    SELECT
        *,
        RANK() OVER (ORDER BY discount_pct_rank_dense_rank DESC NULLS FIRST) AS discount_pct_rank_dense_rank_rank,
        RANK() OVER (ORDER BY discount_pct_rank_dense_rank NULLS LAST) AS discount_pct_rank_dense_rank_rank_2,
        STDDEV_SAMP(starts_on_period_count_discount_pct_percent_rank) OVER (PARTITION BY YEAR(ends_on)) AS ends_on_year_stddev_samp_starts_on_period_count_discount_pct_percent_rank,
        PERCENT_RANK() OVER (ORDER BY starts_on_period_count_discount_pct DESC NULLS LAST) AS starts_on_period_count_discount_pct_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY discount_pct_rank_dense_rank DESC) AS discount_pct_rank_dense_rank_percent_rank
    FROM enriched
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct_rank,
    starts_on_period_count_discount_pct_percent_rank,
    discount_pct_rank_dense_rank,
    ends_on_year_stddev_samp_starts_on_period_count_discount_pct_percent_rank,
    discount_pct_rank_dense_rank_percent_rank
FROM enriched_promotions
ORDER BY starts_on
