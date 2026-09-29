with valid as (
    -- Keep recent, valid rows
    select *, lower(status) as status_lower
    from orders
    where order_date >= date '2023-02-23'
      and store_id is not null
)
-- Join a dimension and aggregate
select
    p.segment,
    p.city,
    s.status,
    count(*) as num_rows,
    min(s.discount) as min_discount,
    sum(s.shipping_fee) as total_shipping_fee,
    grouping_id() as level_id
from valid s
join customers p on s.customer_id = p.id
group by rollup (p.segment, p.city, s.status)
order by num_rows desc nulls first
