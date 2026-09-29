WITH enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.credit_limit,
        p.name,
        p.signup_date
    FROM accounts e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY signup_date DESC, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.amount <= 1900
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        balance,
        credit_limit
    FROM kept
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (balance, credit_limit AS CREDIT_LIMIT_VAL)
)
