with lines as (
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view explode(attributes) x as key, attr_value
),
exploded as (
    select
        id,
        customer_id,
        key,
        explode_outer(coupon_codes) as element
    from lines
),
base as (
    select
        *,
        concat(element, ' ', key) as element_key,
        coalesce(key, 'UNKNOWN') as resolved_key
    from exploded
    where key is not null
),
enriched as (
    select
        a.*,
        b.name,
        b.email,
        b.segment
    from base a
    left join customers b
        on a.customer_id = b.id
    where b.age < 30
),
base_carts as (
    select
        id,
        customer_id,
        key,
        element,
        element_key,
        name,
        email,
        segment,
        concat(name, ', ', key) as name_key,
        coalesce(key, 'NONE') as resolved_key_2
    from enriched
    where customer_id = 25
)
select
    element,
    count(case when element = 'WELCOME5' then 1 end) as welcome5_count,
    sort_array(collect_set(segment)) as segment_list,
    sort_array(collect_set(key)) as key_list,
    count_if(element = 'SPRING10') as spring10_events
from base_carts
group by element;
