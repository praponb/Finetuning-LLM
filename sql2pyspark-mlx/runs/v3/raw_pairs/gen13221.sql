select paid_at, amount, order_id from payments where hour(paid_at) between 9 and 16;
