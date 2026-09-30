select id, paid_at, ntile(10) over (order by paid_at, id) as quartile from payments where to_date(paid_at) < '2025-09-23'
