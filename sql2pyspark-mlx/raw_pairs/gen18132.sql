WITH latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.signup_date,
        s.age,
        top1.device
    FROM customers s
    JOIN LATERAL (
            SELECT c.device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(age, 1, 0) OVER (PARTITION BY MONTH(signup_date), device ORDER BY age, id) AS prev_age,
        AVG(age) OVER (PARTITION BY MONTH(signup_date), device ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_avg_6,
        SUM(age) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        AVG(age) OVER (PARTITION BY device ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS event_seq
    FROM latest_match
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM kept
    EXCEPT
    SELECT id
    FROM customers
    WHERE credit_limit IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_ADD(signup_date, 2) AS signup_date_plus_2d,
        CASE WHEN age_avg_6 >= 62 THEN 'High' WHEN age_avg_6 >= 30 THEN 'Medium' ELSE 'Low' END AS age_avg_6_level
    FROM kept_rows
    WHERE signup_date >= DATE '2023-06-16'
      AND device IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        age_avg_6,
        running_age,
        signup_date_plus_2d,
        age_avg_6_level,
        ROUND(age_avg_5 / SUM(age_avg_5) OVER (PARTITION BY country), 4) AS age_avg_5_share
    FROM valid
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.status IN ('OPEN', 'FROZEN')
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        country,
        DATE_TRUNC('QUARTER', signup_date_plus_2d) AS quarter_start,
        MAX(running_age) AS max_running_age,
        SUM(age_avg_6) FILTER (WHERE country = 'JP') AS jp_total_age_avg_6,
        APPROX_PERCENTILE(running_age, ARRAY(0.25, 0.5, 0.75)) AS running_age_quartiles,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY SUM(age_avg_6) FILTER (WHERE country = 'JP') DESC) AS jp_total_age_avg_6_dense_rank
    FROM eligible
    GROUP BY country, DATE_TRUNC('QUARTER', signup_date_plus_2d)
)
SELECT
    country,
    quarter_start,
    jp_total_age_avg_6,
    running_age_quartiles,
    jp_total_age_avg_6_dense_rank
FROM totals
ORDER BY quarter_start DESC NULLS LAST;
