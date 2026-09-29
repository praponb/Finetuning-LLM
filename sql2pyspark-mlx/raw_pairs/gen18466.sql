WITH not_flagged AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
)
SELECT
    CASE WHEN age >= 56 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    SUM(credit_limit) FILTER (WHERE city = 'Bangkok') AS bangkok_total_credit_limit,
    BIT_OR(age) AS age_bit_or
FROM not_flagged
GROUP BY 1
HAVING bangkok_total_credit_limit >= 0;
