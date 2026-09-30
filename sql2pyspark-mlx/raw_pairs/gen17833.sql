with remaining_keys as (
    -- Set difference on keys
    select id, customer_id
    from orders
    except
    select id, customer_id
    from orders
    where customer_id in (17, 18, 22, 28)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from orders s
    join remaining_keys k on s.id = k.id and s.customer_id = k.customer_id
)
-- Aggregate with conditional metrics
select
    store_id,
    round(sum(shipping_fee) / nullif(count(case when status = 'COMPLETED' then 1 end), 0), 2) as shipping_fee_per_completed,
    count(distinct channel) as distinct_channels,
    round(sum(discount) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as discount_per_mobile
from verified
group by store_id
having count(distinct channel) > 1
order by discount_per_mobile desc
