SELECT COUNT(shipping_fee) AS shipping_fee_count, MAX(discount) - MIN(discount) AS discount_range FROM orders;
