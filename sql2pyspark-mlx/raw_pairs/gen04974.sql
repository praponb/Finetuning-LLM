WITH matched_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM customers
    EXCEPT ALL
    SELECT segment, id
    FROM customers
    WHERE credit_limit < 6900
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.segment = k.segment AND s.id = k.id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT country, city
    FROM reconciled
    INTERSECT
    SELECT country, city
    FROM customers
    WHERE name LIKE '%Che%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys_2 k ON s.country = k.country AND s.city = k.city
),
matched_keys_3 AS (
    -- Set difference on keys
    SELECT id, country
    FROM verified
    EXCEPT
    SELECT id, country
    FROM customers
    WHERE city IN ('Singapore', 'Chiang Mai', 'Bangkok')
),
reconciled_customers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN matched_keys_3 k ON s.id = k.id AND s.country = k.country
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(age, credit_limit) AS least_age_credit_limit,
        LOWER(country) AS country_lower,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM reconciled_customers
    WHERE signup_date > TO_DATE('2023-06-02')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        city,
        segment,
        signup_date,
        is_active,
        age,
        least_age_credit_limit,
        country_lower,
        resolved_city,
        PERCENT_RANK() OVER (PARTITION BY MONTH(signup_date), city ORDER BY age ASC NULLS LAST) AS age_percent_rank
    FROM cleaned
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_recent AS age_percent_rank_2,
        STDDEV_SAMP(age) OVER (PARTITION BY city, segment) AS city_stddev_samp_age,
        RANK() OVER w_recent_2 AS least_age_credit_limit_rank
    FROM enriched
    WHERE segment IN ('Consumer', 'SMB')
    WINDOW w_recent AS (PARTITION BY segment ORDER BY age DESC NULLS FIRST), w_recent_2 AS (PARTITION BY city ORDER BY least_age_credit_limit ASC)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    SUM(city_stddev_samp_age) FILTER (WHERE city = 'Singapore') AS singapore_total_city_stddev_samp_age,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(city_stddev_samp_age, 0.9) AS city_stddev_samp_age_p90,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY SUM(city_stddev_samp_age) FILTER (WHERE city = 'Singapore') DESC) AS singapore_total_city_stddev_samp_age_dense_rank
FROM scored
GROUP BY DATE_TRUNC('MONTH', signup_date), DATE_TRUNC('YEAR', signup_date)
HAVING SUM(city_stddev_samp_age) FILTER (WHERE city = 'Singapore') > 0;
