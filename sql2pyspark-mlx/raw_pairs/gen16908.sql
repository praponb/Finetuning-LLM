with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        tag
    from carts
    lateral view outer explode(coupon_codes) e as tag
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        d.segment,
        d.email,
        d.city
    from exploded a
    inner join customers d
        on a.customer_id = d.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        segment,
        email,
        city,
        length(segment) as segment_length
    from joined
    where customer_id is not null
      and email like '%@mail.co.th'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by segment, city order by segment_length desc nulls last) as segment_length_dense_rank,
        cume_dist() over (partition by city order by segment_length nulls first) as segment_length_cume_dist,
        dense_rank() over (order by segment_length desc) as segment_length_dense_rank_2
    from filtered
)
select
    id,
    segment,
    city,
    segment_length,
    segment_length_dense_rank,
    segment_length_cume_dist,
    segment_length_dense_rank_2
from metrics
