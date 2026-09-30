SELECT last_restock FROM inventory WHERE product_id IN (4, 5, 10) UNION ALL SELECT start_date last_restock FROM subscriptions WHERE monthly_fee > 160;
