with matched_keys as (
    select store_id, channel
    from orders
    intersect
    select store_id, channel
    from orders
    where shipping_fee < 21
),
verified as (
    select s.*
    from orders s
    join matched_keys k on s.store_id = k.store_id and s.channel = k.channel
)
select
    date_trunc('week', order_date) as week_start,
    customer_id,
    case when discount >= 0.3 then 'PREMIUM' else 'STANDARD' end as discount_tier,
    sort_array(collect_set(channel)) as channel_list,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    corr(discount, amount) as corr_discount_amount,
    rank() over (partition by date_trunc('week', order_date) order by corr(discount, amount) desc) as corr_discount_amount_rank
from verified
group by 1, 2, 3
order by customer_id nulls last;
