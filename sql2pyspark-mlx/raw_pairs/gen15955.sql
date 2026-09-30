SELECT * FROM customers QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date DESC, id) = 1;
