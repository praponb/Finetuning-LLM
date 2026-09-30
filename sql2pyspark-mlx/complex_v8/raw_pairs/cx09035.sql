SELECT claim_type AS name, claim_date AS day
FROM claims
UNION ALL
SELECT segment, joined_on
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'L%'
ORDER BY day, name
