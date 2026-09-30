WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        COALESCE(credit_limit, 0) AS credit_limit_filled
    FROM customers
    WHERE name LIKE 'Emm%'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(segment) AS segment_length,
        CAST(is_active AS INT) AS is_active_flag
    FROM valid
    WHERE signup_date > DATE '2023-04-21'
      AND segment IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY credit_limit NULLS LAST) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_active_flag) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_active_flag_avg_3_prior,
        AVG(credit_limit_filled) OVER w_part AS country_avg_credit_limit_filled,
        FIRST_VALUE(age) IGNORE NULLS OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM ranked
    WINDOW w_part AS (PARTITION BY country)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_known_age >= 72 THEN 'HIGH' ELSE 'LOW' END AS first_known_age_tier,
        country,
        segment,
        SUM(CASE WHEN segment = 'Corporate' THEN country_avg_credit_limit_filled ELSE 0 END) AS corporate_country_avg_credit_limit_filled,
        PERCENTILE_APPROX(is_active_flag_avg_3_prior, 0.5) AS is_active_flag_avg_3_prior_p50,
        ROUND(AVG(first_known_age), 2) AS avg_first_known_age,
        MAX(first_known_age) FILTER (WHERE segment = 'SMB') AS smb_max_first_known_age,
        CORR(age, credit_limit) AS corr_age_credit_limit,
        ROUND(SUM(credit_limit_filled) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS credit_limit_filled_per_de
    FROM with_windows
    GROUP BY 1, 2, 3
)
SELECT
    first_known_age_tier,
    country,
    segment,
    is_active_flag_avg_3_prior_p50,
    smb_max_first_known_age,
    corr_age_credit_limit,
    credit_limit_filled_per_de,
    CASE WHEN country RLIKE '^(SG|JP)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS country_zone
FROM totals
ORDER BY segment DESC NULLS LAST
