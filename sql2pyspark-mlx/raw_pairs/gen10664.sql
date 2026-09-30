WITH kept_keys AS (
    SELECT segment
    FROM customers
    INTERSECT
    SELECT segment
    FROM customers
    WHERE LENGTH(email) >= 7
),
reconciled AS (
    SELECT s.*
    FROM customers s
    JOIN kept_keys k ON s.segment = k.segment
)
SELECT
    country,
    BIT_AND(age) AS age_bit_and,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    COUNT_IF(segment = 'SMB') AS smb_events,
    MIN(credit_limit) AS min_credit_limit
FROM reconciled
GROUP BY 1
HAVING age_bit_and >= 1
