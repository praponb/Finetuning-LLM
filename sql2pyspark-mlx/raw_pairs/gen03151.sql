WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        is_active,
        credit_limit,
        age,
        LAST_VALUE(age, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date), segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age,
        LAG(age, 1) OVER w_recent AS prev_age
    FROM customers
    WINDOW w_recent AS (PARTITION BY MONTH(signup_date), city ORDER BY signup_date, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit >= 17700 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    AVG(age) AS avg_age,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(prev_age), 2) AS avg_prev_age,
    BIT_AND(age) AS age_bit_and,
    ROUND(AVG(age), 2) AS avg_age_2
FROM enriched
GROUP BY 1
HAVING ROUND(AVG(age), 2) >= 0
ORDER BY credit_limit_tier NULLS LAST;
