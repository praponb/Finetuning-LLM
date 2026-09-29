select round(avg(quantity), 2) avg_quantity, min(quantity) min_quantity from order_items order by min_quantity desc, avg_quantity desc
