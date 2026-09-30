select status, upper(status) as status_upper from payments where (order_id = 8 and to_date(paid_at) >= '2025-05-01') or status = 'SUCCESS' order by status_upper desc, status desc;
