SELECT * FROM (SELECT YEAR(order_date) order_year, MAX(discount) max_discount, COUNT(DISTINCT shipping_fee) unique_channels FROM orders GROUP BY YEAR(order_date)) sub WHERE max_discount > 0.22
