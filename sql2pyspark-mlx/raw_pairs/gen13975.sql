WITH valid AS (
    SELECT
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        credit_limit * age AS credit_limit_times_age,
        COALESCE(segment, 'N/A') AS resolved_segment
    FROM customers
    WHERE signup_date >= DATE '2023-07-16'
)
SELECT *
FROM (
    SELECT
        country,
        segment,
        credit_limit
    FROM valid
) src
PIVOT (
    AVG(credit_limit)
    FOR segment IN ('Corporate', 'Consumer')
)
