with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view explode(attributes) t as attr_key, v
),
lines_carts as (
    -- Unnest the array into one row per element
    select
        id,
        attr_key,
        v,
        explode_outer(coupon_codes) as code
    from lines
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        length(attr_key) as attr_key_length,
        coalesce(attr_key, 'UNKNOWN') as resolved_attr_key
    from lines_carts
    where attr_key != 'utm_source'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        attr_key,
        v,
        dense_rank() over (order by attr_key_length asc nulls first) as attr_key_length_dense_rank
    from cleaned
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        case when attr_key_length_dense_rank >= 7 then 'High' when attr_key_length_dense_rank >= 3 then 'Medium' else 'Low' end as attr_key_length_dense_rank_tier,
        coalesce(attr_key_length_dense_rank, 0) as attr_key_length_dense_rank_filled
    from with_windows
    where attr_key = 'utm_source'
),
cleaned_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when attr_key_length_dense_rank_filled >= 90 then 'High' when attr_key_length_dense_rank_filled >= 19 then 'Medium' else 'Low' end as attr_key_length_dense_rank_filled_tier,
        attr_key_length_dense_rank / nullif(attr_key_length_dense_rank_filled, 0) as attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled,
        coalesce(attr_key, 'DIRECT') as resolved_attr_key
    from base
    where attr_key is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        v,
        attr_key_length_dense_rank_tier,
        attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled,
        resolved_attr_key,
        rank() over (partition by attr_key order by attr_key_length_dense_rank desc nulls last) as attr_key_length_dense_rank_rank,
        count(attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled) over w_recent as attr_key_count_attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled,
        rank() over w_recent_2 as attr_key_length_dense_rank_rank_2,
        min(attr_key_length_dense_rank_filled) over w_recent as attr_key_min_attr_key_length_dense_rank_filled,
        cume_dist() over w_hist as attr_key_length_dense_rank_cume_dist
    from cleaned_carts
    window w_recent as (partition by attr_key), w_recent_2 as (partition by attr_key order by attr_key_length_dense_rank asc nulls first), w_hist as (order by attr_key_length_dense_rank desc nulls first)
),
report as (
    select
        id,
        v,
        attr_key_length_dense_rank_tier,
        attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled,
        resolved_attr_key,
        attr_key_length_dense_rank_rank_2,
        attr_key_min_attr_key_length_dense_rank_filled,
        attr_key_length_dense_rank_cume_dist
    from scored
)
select
    id,
    v,
    attr_key_length_dense_rank_tier,
    attr_key_length_dense_rank_per_attr_key_length_dense_rank_filled,
    resolved_attr_key,
    attr_key_length_dense_rank_cume_dist
from report;
