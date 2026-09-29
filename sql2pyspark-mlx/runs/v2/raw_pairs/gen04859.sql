WITH src AS (
    SELECT *
    FROM customers
    WHERE country <> 'SG'
),
Agg AS (
    SELECT is_active, SORT_ARRAY(COLLECT_SET(country)) AS country_list, MIN(credit_limit) AS min_credit_limit
    FROM src
    GROUP BY is_active
)
SELECT *
FROM Agg
WHERE min_credit_limit < 11500
ORDER BY min_credit_limit DESC, is_active ASC
LIMIT 1
