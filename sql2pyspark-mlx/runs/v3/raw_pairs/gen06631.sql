SELECT units, product_id, store_id
FROM store_sales
WHERE NOT (MONTH(sale_date) IN (1, 8, 12) AND (revenue BETWEEN 1200 AND 2600 OR revenue <= 1200));
