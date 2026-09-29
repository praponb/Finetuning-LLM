SELECT * FROM accounts QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) = 1;
