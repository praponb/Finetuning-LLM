SELECT segment, country FROM customers INTERSECT SELECT segment, country FROM customers WHERE signup_date <= '2024-08-06';
