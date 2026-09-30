select id, method, order_id, hour(paid_at) paid_at_hour from payments order by paid_at_hour;
