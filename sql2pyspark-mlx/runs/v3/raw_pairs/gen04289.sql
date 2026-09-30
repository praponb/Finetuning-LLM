SELECT monthly_fee, 'subscription' AS source FROM subscriptions UNION ALL SELECT unit_price AS monthly_fee, 'order_item' AS source FROM order_items;
