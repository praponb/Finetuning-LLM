select order_id, max(paid_at) as last_paid_at, round(avg(amount), 2) as avg_amount from payments where status = 'REFUNDED' or to_date(paid_at) < '2024-07-31' group by order_id;
