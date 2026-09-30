WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(discount_pct * 0.85, 2) AS adjusted_discount_pct,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM promotions
    WHERE channel IS NOT DISTINCT FROM 'web'
      AND channel = 'web'
),
filtered_promotions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        channel_clean,
        DATEDIFF(CURRENT_DATE(), ends_on) / 30 AS months_since_ends_on,
        COALESCE(channel, 'UNKNOWN') AS channel_clean_2
    FROM filtered
    WHERE channel IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT promo_code, id
    FROM filtered_promotions
    EXCEPT ALL
    SELECT promo_code, id
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'VIP')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_promotions s
    JOIN remaining_keys k ON s.promo_code = k.promo_code AND s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        channel_clean_2,
        RANK() OVER (PARTITION BY promo_code ORDER BY months_since_ends_on DESC NULLS LAST) AS months_since_ends_on_rank,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM kept_rows
    WHERE promo_code = 'BUNDLE'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        promo_code,
        ends_on,
        channel_clean_2,
        months_since_ends_on_rank,
        discount_pct_share,
        ROUND(months_since_ends_on_rank / SUM(months_since_ends_on_rank) OVER (PARTITION BY promo_code), 4) AS months_since_ends_on_rank_share,
        MAX(discount_pct_share) OVER (PARTITION BY DATE_TRUNC('week', ends_on)) AS ends_on_period_max_discount_pct_share
    FROM scored
),
scored_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(months_since_ends_on_rank_share) OVER (PARTITION BY DATE_TRUNC('MONTH', ends_on), promo_code) AS ends_on_period_count_months_since_ends_on_rank_share,
        COUNT(discount_pct_share) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct_share,
        DENSE_RANK() OVER (PARTITION BY MONTH(ends_on), promo_code ORDER BY months_since_ends_on_rank_share) AS months_since_ends_on_rank_share_dense_rank,
        STDDEV_SAMP(ends_on_period_max_discount_pct_share) OVER (PARTITION BY YEAR(ends_on), promo_code) AS ends_on_year_stddev_samp_ends_on_period_max_discount_pct_share,
        STDDEV_SAMP(months_since_ends_on_rank) OVER (PARTITION BY promo_code) AS promo_code_stddev_samp_months_since_ends_on_rank
    FROM metrics
)
SELECT
    promo_code,
    ends_on,
    channel_clean_2,
    months_since_ends_on_rank,
    discount_pct_share,
    months_since_ends_on_rank_share,
    ends_on_period_max_discount_pct_share,
    ends_on_period_count_months_since_ends_on_rank_share,
    promo_code_count_discount_pct_share
FROM scored_promotions
ORDER BY promo_code DESC;
