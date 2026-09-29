WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        LAST_DAY(signup_date) AS signup_date_month_end,
        COALESCE(name, 'N/A') AS name_filled,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM customers
    WHERE city IS NOT NULL
      AND signup_date >= DATE '2023-09-05'
),
cleaned_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        segment,
        signup_date,
        age,
        signup_date_month_end,
        name_filled,
        segment_clean,
        SUBSTRING(country, 1, 3) AS country_prefix,
        COALESCE(city, 'NONE') AS city_filled
    FROM cleaned
    WHERE city IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(DATEDIFF(signup_date_month_end, signup_date) / 30, 1) AS months_signup_date_to_signup_date_month_end
    FROM cleaned_customers
    WHERE signup_date_month_end BETWEEN '2023-05-01' AND '2023-09-23'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY signup_date DESC NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(city) AS clean_city,
        CONCAT(segment, '/', country) AS segment_country,
        COALESCE(city, 'OTHER') AS city_clean
    FROM ranked
    WHERE city IS NOT NULL
)
SELECT
    id,
    country,
    signup_date,
    age,
    signup_date_month_end,
    name_filled,
    segment_clean,
    country_prefix,
    city_filled,
    CASE WHEN country RLIKE '^(JP|TH)$' THEN 'CORE' WHEN country RLIKE '^(US|SG)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS country_zone
FROM valid_customers
ORDER BY country_zone DESC NULLS LAST;
