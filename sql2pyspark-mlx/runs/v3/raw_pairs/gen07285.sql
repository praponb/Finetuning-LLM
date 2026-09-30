select status, 'order' as source from orders where customer_id is null union select status, 'payment' as source from payments order by source desc
