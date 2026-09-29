SELECT unit_price AS value FROM order_items UNION SELECT monthly_fee AS value FROM subscriptions;
