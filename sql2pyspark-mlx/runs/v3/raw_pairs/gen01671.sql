SELECT o.amount, s.region, s.store_name, s.opened_date FROM orders o FULL OUTER JOIN stores s ON s.id = o.store_id WHERE s.sqft < 15097;
