WITH src AS (
    SELECT *, SUBSTRING(merchant, 1, 3) merchant_prefix
    FROM transactions
    WHERE merchant IN ('Shell', 'Grab') AND amount BETWEEN 2700 AND 2800
),
Summary AS (
    SELECT is_flagged, COUNT(DISTINCT amount) unique_merchants, MAX(amount) - MIN(amount) amount_range
    FROM src
    GROUP BY is_flagged
)
SELECT is_flagged, amount_range
FROM Summary
