SELECT monthly_fee, 'subscription' AS source FROM subscriptions WHERE monthly_fee > 120 UNION SELECT unit_price, 'order_item' AS source FROM order_items WHERE product_id IN (3, 36)
