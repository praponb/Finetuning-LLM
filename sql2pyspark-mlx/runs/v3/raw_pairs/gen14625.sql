SELECT revenue FROM store_sales WHERE product_id IN (5, 9, 10) INTERSECT SELECT monthly_fee FROM subscriptions WHERE customer_id IN (5, 14) ORDER BY revenue DESC
