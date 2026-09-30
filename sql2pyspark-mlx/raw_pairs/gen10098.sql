with ranked_subscriptions as (select *, row_number() over (partition by customer_id order by monthly_fee desc, id) as rnk from subscriptions) select * from ranked_subscriptions where rnk <= 2;
