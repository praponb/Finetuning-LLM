WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct, 1) AS discount_pct_rounded,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE starts_on > DATE '2023-05-16'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(ends_on) AS ends_on_dayofweek,
        ROUND(discount_pct * 0.85, 2) AS adjusted_discount_pct
    FROM valid
    WHERE ends_on > TO_DATE('2023-06-29')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_rounded,
        channel_filled,
        ends_on_dayofweek,
        adjusted_discount_pct,
        AVG(discount_pct_rounded) OVER (PARTITION BY promo_code ORDER BY starts_on ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_rounded_avg_6,
        SUM(adjusted_discount_pct) OVER (ORDER BY adjusted_discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_adjusted_discount_pct,
        AVG(discount_pct) OVER w_grp AS channel_avg_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY adjusted_discount_pct DESC NULLS FIRST) AS adjusted_discount_pct_percent_rank
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY channel)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(ends_on_dayofweek AS STRING) AS ends_on_dayofweek_num
    FROM enriched
    WHERE adjusted_discount_pct_percent_rank BETWEEN 0.37 AND 0.55
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY cumulative_adjusted_discount_pct DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_rounded,
    channel_filled,
    ends_on_dayofweek,
    adjusted_discount_pct
FROM ranked
ORDER BY discount_pct_rounded DESC NULLS FIRST
