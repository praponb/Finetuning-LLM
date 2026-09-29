select id, paid_at, amount, concat_ws('/', status, method) as status_method_key from payments where method in ('paypal', 'bank_transfer') and amount > 770;
