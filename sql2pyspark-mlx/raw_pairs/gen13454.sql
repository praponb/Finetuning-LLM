with enriched as (
    select
        *,
        lead(sqft) over w_running as next_sqft,
        lag(sqft) over w_running as prev_sqft,
        row_number() over w_running as row_seq
    from stores
    window w_running as (partition by region order by opened_date asc, id asc)
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    next_sqft,
    prev_sqft,
    row_seq
from enriched
order by prev_sqft asc
