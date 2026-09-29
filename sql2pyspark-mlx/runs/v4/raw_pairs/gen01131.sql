select p.order_id, o.store_id from payments p inner join orders o on p.order_id = o.id where p.amount between 860 and 1050 or to_date(p.paid_at) < '2023-10-01'
