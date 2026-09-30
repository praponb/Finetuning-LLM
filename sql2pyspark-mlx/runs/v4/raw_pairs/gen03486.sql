select p.brand, count(oi.unit_price) as unit_price_count, round(stddev(oi.unit_price), 2) as stddev_unit_price from order_items oi right join products p on oi.product_id = p.id group by p.brand;
