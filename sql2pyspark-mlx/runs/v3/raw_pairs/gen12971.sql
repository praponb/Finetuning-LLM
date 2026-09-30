SELECT opened_date
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CHECKING', 'CREDIT')
UNION
SELECT sale_date
FROM store_sales
