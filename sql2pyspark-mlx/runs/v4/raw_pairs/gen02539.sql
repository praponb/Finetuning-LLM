SELECT shipped_date FROM shipments WHERE shipped_date >= '2025-09-11' EXCEPT SELECT signup_date FROM customers WHERE signup_date BETWEEN '2025-08-16' AND '2025-10-01';
