select paid_at, order_id, amount, lower(method) method_lower from payments where status = 'SUCCESS' and paid_at < '2023-05-08' order by paid_at desc;
