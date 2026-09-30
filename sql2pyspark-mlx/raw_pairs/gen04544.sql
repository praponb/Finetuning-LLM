WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        CASE WHEN country IN ('TH', 'SG') THEN 'Priority' ELSE 'Other' END country_group
    FROM customers
    WHERE signup_date >= TO_DATE('2023-03-22')
),
with_windows AS (
    SELECT
        id,
        country,
        segment,
        signup_date,
        credit_limit,
        country_group,
        COUNT(credit_limit) OVER w_grp running_count_credit_limit,
        AVG(credit_limit) OVER w_grp running_avg_credit_limit,
        MIN(credit_limit) OVER w_grp running_min_credit_limit,
        SUM(credit_limit) OVER w_grp running_sum_credit_limit
    FROM base
    WHERE name IS NOT NULL
    WINDOW w_grp AS (PARTITION BY country, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_avg_credit_limit NULLS FIRST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
ranked AS (
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) = 1
),
metrics AS (
    SELECT
        *,
        SUM(running_min_credit_limit) OVER (PARTITION BY country ORDER BY running_count_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_running_min_credit_limit,
        AVG(running_min_credit_limit) OVER (PARTITION BY country ORDER BY running_count_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_running_min_credit_limit,
        MIN(running_min_credit_limit) OVER (PARTITION BY country ORDER BY running_count_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_running_min_credit_limit,
        COUNT(running_min_credit_limit) OVER (PARTITION BY country ORDER BY running_count_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_running_min_credit_limit
    FROM ranked
),
kept_keys AS (
    SELECT segment, country
    FROM metrics
    EXCEPT
    SELECT segment, country
    FROM customers
    WHERE email IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.country = k.country
),
kept AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.country = 'TH'
        )
)
SELECT
    id,
    country,
    segment,
    signup_date,
    country_group,
    running_count_credit_limit,
    running_sum_credit_limit,
    rank_in_group
FROM kept
ORDER BY running_sum_credit_limit DESC;
