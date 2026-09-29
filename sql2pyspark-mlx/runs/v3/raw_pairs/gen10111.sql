select category, count(distinct stock_qty) as unique_suppliers, sum(price) as total_price from products where upper(trim(product_name)) = 'DESK LAMP' group by category having unique_suppliers > 3;
