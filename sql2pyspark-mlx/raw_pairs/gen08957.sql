with valid as (
    -- Basic cleansing and defaults
    select
        *,
        amount * discount as amount_times_discount,
        floor(amount) as amount_whole
    from orders
    where channel is not null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and hour(ch.paid_at) between 9 and 14
        )
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        customer_id,
        amount,
        amount_whole
    from (
        select *, row_number() over (partition by store_id, status order by amount_times_discount nulls first, id) as latest_rank
        from not_flagged
    ) ranked
    where latest_rank = 1
),
joined as (
    -- Attach customers attributes
    select src.*, p.segment
    from ranked src
    left join customers p
        on src.customer_id = p.id
)
-- Join a dimension and aggregate
select
    dim.city,
    dim.country,
    e.segment as order_segment,
    count(*) as num_rows,
    min(e.amount_whole) as min_amount_whole
from joined e
left join customers dim on e.customer_id = dim.id
group by dim.city, dim.country, e.segment;
