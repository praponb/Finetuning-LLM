select id, amount, cast(amount as int) as amount_int from orders where status = 'COMPLETED' and discount between 0.31 and 0.38 and shipping_fee < 22;
