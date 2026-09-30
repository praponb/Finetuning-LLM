select status, customer_id from orders where amount < 1560 union all select status, customer_id from orders where shipping_fee between 28 and 32
