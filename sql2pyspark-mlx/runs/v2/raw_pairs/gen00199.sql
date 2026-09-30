SELECT
    id,
    customer_id,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS opened_date_row
FROM accounts;
