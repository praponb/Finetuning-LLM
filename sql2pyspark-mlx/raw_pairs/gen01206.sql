with lines as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) li as attr_key, attr_value
),
joined as (
    select
        a.id,
        a.customer_id,
        a.attr_key,
        a.attr_value,
        c.name,
        c.city,
        c.segment
    from lines a
    left join customers c
        on a.customer_id <=> c.id
    where c.age between 41 and 46
),
cleaned as (
    select
        *,
        concat(segment, ', ', attr_key) as segment_attr_key,
        lower(attr_key) as attr_key_lower,
        coalesce(city, 'OTHER') as city_clean
    from joined
    where attr_key is distinct from 'gift'
      and customer_id in (24, 33)
),
matched_keys as (
    select customer_id, id
    from cleaned
    except distinct
    select customer_id, id
    from carts
    where customer_id = 15
),
reconciled as (
    select s.*
    from cleaned s
    inner join matched_keys k on s.customer_id = k.customer_id and s.id = k.id
)
select
    segment,
    count(*) filter (where city = 'Berlin') as berlin_rows,
    count(*) as row_count,
    sort_array(collect_set(attr_key)) as attr_key_list,
    count(distinct city) as distinct_cities
from reconciled
group by segment
order by row_count;
