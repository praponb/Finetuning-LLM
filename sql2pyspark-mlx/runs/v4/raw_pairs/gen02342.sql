with flattened_attrs as (
    select
        id,
        coupon_codes,
        ship_to,
        aggregate(items, 0D, (acc, x) -> acc + x.qty * x.price, acc -> round(acc, 2)) as cart_value,
        attributes['utm_source'] as utm_source,
        ship_to.city as ship_city
    from carts
)
select
    ship_city,
    case when cart_value >= 1570 then 'PREMIUM' else 'STANDARD' end as cart_value_tier,
    max(cart_value) filter (where ship_city = 'Singapore') as singapore_max_cart_value,
    sort_array(collect_set(ship_city)) as ship_city_list,
    count(distinct id) as distinct_ids,
    rank() over (partition by ship_city order by count(distinct id) desc) as distinct_ids_rank
from flattened_attrs
group by 1, 2
