select id, amount, concat(method, '/', status) as method_status, upper(status) as status_upper from payments where order_id = 33 order by amount
