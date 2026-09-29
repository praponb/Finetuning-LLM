select id, customer_id from accounts where customer_id = 3 union all select id, customer_id from accounts where status in ('OPEN', 'FROZEN') order by id desc
