WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        ADD_MONTHS(ends_on, 1) AS ends_on_plus_months,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-05-09')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 10)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(channel, 1, 2) AS channel_prefix,
        LEAST(discount_pct, promo_id_2) AS least_discount_pct_promo_id_2,
        COALESCE(promotion_promo_code, 'NONE') AS promotion_promo_code_filled
    FROM matched
    WHERE promo_id_2 > 9
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        promo_code,
        COUNT(DISTINCT promotion_promo_code) AS distinct_promotion_promo_codes,
        STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
        COUNT(*) AS num_rows,
        ROUND(AVG(least_discount_pct_promo_id_2), 2) AS avg_least_discount_pct_promo_id_2,
        GROUPING_ID() AS grouping_id
    FROM valid
    GROUP BY CUBE (channel, promo_code)
)
SELECT channel, num_rows
FROM summary
ORDER BY channel;
