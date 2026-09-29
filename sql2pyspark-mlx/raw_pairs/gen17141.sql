select amount, order_id, paid_at, cast(amount as int) amount_int from payments where paid_at < '2024-03-02' and method in ('promptpay', 'card', 'bank_transfer') order by amount_int desc, order_id;
