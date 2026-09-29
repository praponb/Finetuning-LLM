select status, round(avg(amount), 2) as avg_amount from payments where paid_at < '2024-10-20' and order_id = 15 group by status;
