WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(account_type, ' ', status) account_type_status
    FROM accounts
    WHERE opened_date > TO_DATE('2023-09-03')
      AND status = 'CLOSED'
)
-- Join a dimension and aggregate
SELECT
    p.country,
    src.status,
    COUNT(*) AS num_rows,
    MAX(src.balance) AS max_balance,
    GROUPING_ID() AS gid
FROM cleaned src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY CUBE (p.country, src.status)
ORDER BY num_rows DESC NULLS FIRST
