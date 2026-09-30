select id, order_id, amount, length(status) as status_length, case when method = 'paypal' then 1 else 0 end as is_paypal from payments order by status_length;
