SELECT opened_date, 'store' AS source FROM stores WHERE region IN ('North', 'East', 'South') UNION SELECT signup_date AS opened_date, 'customer' AS source FROM customers;
