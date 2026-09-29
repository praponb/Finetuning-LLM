SELECT opened_date FROM accounts WHERE opened_date IS NOT NULL UNION SELECT sale_date AS opened_date FROM store_sales WHERE sale_date IS NOT NULL;
