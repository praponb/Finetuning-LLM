WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, REGEXP_REPLACE(name, ' ', '') AS name_slug
    FROM customers
    WHERE signup_date > DATE '2023-08-08'
      AND city != 'Bangkok'
),
kept_keys AS (
    -- Set difference on keys
    SELECT segment
    FROM filtered
    EXCEPT DISTINCT
    SELECT segment
    FROM customers
    WHERE country IN ('US', 'TH', 'JP')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.segment = k.segment
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us
    FROM kept_rows
    WHERE city IS NOT NULL
      AND city IN ('Bangkok', 'Tokyo', 'London')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date BETWEEN '2024-03-11' AND '2024-11-19'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        country,
        is_active,
        credit_limit,
        age,
        name_slug,
        CORR(credit_limit, age) OVER (PARTITION BY country, city) AS corr_credit_limit_age,
        CUME_DIST() OVER w_part AS is_us_cume_dist,
        CORR(credit_limit, age) OVER (PARTITION BY segment, city) AS corr_credit_limit_age_2
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY country ORDER BY is_us DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_us_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS is_us_cume_dist_tier,
    MIN(corr_credit_limit_age) AS min_corr_credit_limit_age,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SUM(is_us_cume_dist) FILTER (WHERE country = 'DE') AS de_total_is_us_cume_dist
FROM metrics
GROUP BY CASE WHEN is_us_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END
HAVING num_countries < 1000
ORDER BY num_countries DESC;
