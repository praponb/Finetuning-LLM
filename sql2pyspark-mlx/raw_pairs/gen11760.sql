select category, count(*) as total_count from products where cost < 220 and product_name not like '%Can%' and category not in ('Electronics', 'Fashion') group by category order by category
