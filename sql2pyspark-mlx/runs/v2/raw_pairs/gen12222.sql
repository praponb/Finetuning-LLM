select id, amount, method, order_id, datediff(current_date(), paid_at) as days_since_paid_at, substring(status, 1, 2) as status_prefix from payments where amount between 960 and 1220;
