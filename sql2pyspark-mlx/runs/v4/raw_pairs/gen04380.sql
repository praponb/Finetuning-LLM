WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        DATE_TRUNC('year', signup_date) AS signup_date_period,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM customers
    WHERE UPPER(TRIM(name)) = 'MIA ROSSI'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS credit_limit_avg_5,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN signup_date_period - LAG(signup_date_period) OVER (PARTITION BY country ORDER BY signup_date_period, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY signup_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(signup_date_period) OVER (PARTITION BY country ORDER BY signup_date_period, id) IS NULL
                 OR signup_date_period > LAG(signup_date_period) OVER (PARTITION BY country ORDER BY signup_date_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
sessions_customers AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY signup_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        segment,
        burst_id
    FROM sessions_customers
) src
PIVOT (
    AVG(burst_id) AS value_sum
    FOR segment IN ('Corporate', 'SMB')
);
