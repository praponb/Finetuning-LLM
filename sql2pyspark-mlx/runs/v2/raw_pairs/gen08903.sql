select country, city from customers where is_active = false intersect select country, city from suppliers
