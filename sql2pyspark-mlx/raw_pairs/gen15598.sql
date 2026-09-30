with flattened as (
    select
        id,
        customer_id,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) e as k, v
)
select
    customer_id,
    k,
    count(distinct customer_id) as distinct_customer_ids,
    size(collect_set(k)) as num_ks,
    count(*) as event_count,
    count(*) filter (where k = 'utm_source') as utm_source_rows,
    count(distinct customer_id) as distinct_customer_ids_2,
    size(collect_set(k)) as num_ks_2
from flattened
group by 1, 2
order by utm_source_rows nulls first;
