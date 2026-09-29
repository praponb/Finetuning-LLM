select unit_price from order_items where unit_price <= 170 union all select monthly_fee from subscriptions where start_date >= '2024-06-14' order by unit_price;
