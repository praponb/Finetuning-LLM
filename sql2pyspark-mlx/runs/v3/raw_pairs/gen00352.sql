SELECT sale_date AS event_date
FROM store_sales
UNION ALL
SELECT opened_date AS event_date
FROM stores
WHERE opened_date > '2023-05-01'
