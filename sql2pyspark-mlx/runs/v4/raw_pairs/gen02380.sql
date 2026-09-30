select
    *,
    sum(discount) over (partition by channel order by amount, id rows between unbounded preceding and current row) as cumulative_discount,
    avg(shipping_fee) over (partition by status, channel order by discount, id rows between 2 preceding and 1 preceding) as shipping_fee_avg_2_prior,
    ntile(5) over (partition by status, customer_id order by amount desc, id) as amount_bucket,
    avg(amount) over (partition by status order by amount, id rows between 6 preceding and 1 preceding) as amount_avg_6_prior,
    avg(shipping_fee) over (partition by channel order by discount, id rows between 2 preceding and current row) as shipping_fee_avg_2
from orders;
