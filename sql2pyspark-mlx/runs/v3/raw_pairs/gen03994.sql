select * from (select *, row_number() over (partition by customer_id order by monthly_fee desc, id) as rn from subscriptions) as t where rn <= 3 order by end_date;
