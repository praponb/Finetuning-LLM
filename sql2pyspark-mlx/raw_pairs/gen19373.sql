select id, order_id, amount, upper(status) as status_upper, concat_ws('-', status, method) as status_method_key from payments
