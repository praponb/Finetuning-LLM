WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(age, 0) AS age_rounded
    FROM customers
    WHERE signup_date >= DATE '2023-09-05'
      AND city IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age_rounded DESC, id) AS event_seq,
        RANK() OVER (PARTITION BY country ORDER BY age_rounded NULLS FIRST) AS age_rounded_rank
    FROM valid
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.end_date < s.signup_date
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT segment
    FROM eligible
    INTERSECT ALL
    SELECT segment
    FROM customers
    WHERE age < 37
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    JOIN matched_keys k ON s.segment = k.segment
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM kept_rows
    INTERSECT
    SELECT id, city
    FROM customers
    WHERE segment IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
metrics_customers AS (
    -- Window metrics over several different partitions
    SELECT
        country,
        city,
        segment,
        is_active,
        age,
        age_rounded,
        event_seq,
        age_rounded_rank,
        COUNT(age_rounded_rank) OVER (PARTITION BY city) AS city_count_age_rounded_rank
    FROM verified
    WHERE LENGTH(name) < 9
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        country,
        city,
        segment,
        is_active,
        age,
        age_rounded,
        event_seq,
        age_rounded_rank,
        CAST(is_active AS INT) AS is_active_flag
    FROM metrics_customers
    WHERE segment IS NOT NULL
      AND city IS NOT DISTINCT FROM 'London'
)
-- Aggregate with conditional metrics
SELECT
    country,
    CASE WHEN age_rounded_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS age_rounded_rank_tier,
    BIT_AND(age) AS age_bit_and,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM valid_customers
GROUP BY 1, 2
ORDER BY num_cities DESC;
