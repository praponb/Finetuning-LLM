SELECT sale_date AS event_date, 'store_sale' AS kind
FROM store_sales
WHERE sale_date IS NOT NULL
UNION
SELECT opened_date AS event_date, 'account' AS kind
FROM accounts
WHERE opened_date IS NOT NULL;
