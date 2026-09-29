select *, date_sub(end_date, 57) as end_date_minus_57d from subscriptions order by customer_id desc;
