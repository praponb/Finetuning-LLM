select * from payments where status = 'SUCCESS' and order_id = 27 and amount <= 970 order by status, id limit 3
