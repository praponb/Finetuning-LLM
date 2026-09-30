with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when status = 'EXPIRED' then 1 else 0 end as is_expired,
        length(status) as status_length
    from subscriptions
    where customer_id is not null
)
-- Join a dimension and aggregate
select
    c.segment,
    c.country,
    count(*) as cnt,
    sum(a.is_expired) as total_is_expired,
    min(a.status_length) as min_status_length,
    grouping_id() as gid
from filtered a
inner join customers c on a.customer_id = c.id
group by grouping sets ((c.segment, c.country), (c.segment), ())
order by cnt desc nulls last;
