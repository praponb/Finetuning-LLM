select city from customers where signup_date between '2023-06-20' and '2025-08-07' intersect select supplier_name from suppliers where country != 'US';
