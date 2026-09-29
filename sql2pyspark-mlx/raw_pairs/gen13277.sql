WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        signup_date,
        is_active,
        credit_limit,
        age,
        LEAD(credit_limit, 1) OVER (PARTITION BY city, country ORDER BY age, id) AS next_credit_limit,
        LAG(credit_limit, 1) OVER (PARTITION BY MONTH(signup_date), city ORDER BY age, id) AS prev_credit_limit
    FROM customers
    WHERE city IN ('Tokyo', 'London')
)
SELECT
    id,
    is_active,
    credit_limit,
    age
FROM metrics
