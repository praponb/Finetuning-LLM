WITH filtered AS (
    SELECT *, UPPER(TRIM(name)) AS name_normalized
    FROM customers
    WHERE signup_date >= TO_DATE('2023-05-04')
      AND UPPER(email) LIKE '%LIC%'
),
latest AS (
    SELECT
        id,
        name,
        email,
        country,
        segment,
        is_active,
        credit_limit,
        age
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY age NULLS FIRST) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    SELECT
        id,
        name,
        segment,
        is_active,
        credit_limit,
        age,
        SUBSTRING(email, 1, 4) AS email_prefix
    FROM latest
    WHERE segment IS NOT NULL
      AND segment IS DISTINCT FROM 'Corporate'
),
latest_customers AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment ORDER BY age DESC) AS latest_rank
        FROM cleaned
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        AVG(credit_limit) OVER w_running AS last7_avg_credit_limit,
        SUM(credit_limit) OVER w_running AS last7_sum_credit_limit,
        MIN(credit_limit) OVER w_running AS last7_min_credit_limit
    FROM latest_customers
    WHERE latest_rank != 1
    WINDOW w_running AS (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        id,
        credit_limit,
        age,
        email_prefix,
        latest_rank,
        last7_avg_credit_limit,
        last7_sum_credit_limit,
        SUM(age) OVER w_recent AS running_sum_age,
        MIN(age) OVER w_recent AS running_min_age,
        AVG(age) OVER w_recent AS running_avg_age,
        MAX(age) OVER w_recent AS running_max_age
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    SELECT
        CASE WHEN last7_avg_credit_limit >= 13300 THEN 'LARGE' ELSE 'SMALL' END AS last7_avg_credit_limit_tier,
        APPROX_PERCENTILE(running_max_age, ARRAY(0.25, 0.5, 0.75)) AS running_max_age_quartiles,
        SUM(last7_avg_credit_limit) AS total_last7_avg_credit_limit
    FROM scored
    GROUP BY 1
)
SELECT last7_avg_credit_limit_tier, running_max_age_quartiles
FROM stats
