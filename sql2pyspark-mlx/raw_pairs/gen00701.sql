WITH Src AS (
    SELECT *
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'FAILED') AND method <> 'bank_transfer'
),
amount_bucket_summary AS (
    SELECT CASE WHEN amount >= 1790 THEN 'high' WHEN amount >= 710 THEN 'mid' ELSE 'low' END AS amount_bucket, MAX(amount) - MIN(amount) AS amount_range
    FROM Src
    GROUP BY CASE WHEN amount >= 1790 THEN 'high' WHEN amount >= 710 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM amount_bucket_summary
WHERE amount_range > 52
ORDER BY amount_bucket, amount_range DESC;
