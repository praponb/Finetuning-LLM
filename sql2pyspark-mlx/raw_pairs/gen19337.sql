SELECT
    id,
    customer_id,
    opened_date,
    status,
    CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'Priority' ELSE 'Other' END AS status_group,
    CAST(balance AS INT) AS balance_int
FROM accounts;
