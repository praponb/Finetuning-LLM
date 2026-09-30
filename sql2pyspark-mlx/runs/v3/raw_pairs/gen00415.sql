with valid_customers as (
    select *
    from customers
    where signup_date between '2024-08-23' and '2025-01-01'
),
customers_stats as (
    select segment, sort_array(collect_set(segment)) as segment_list
    from valid_customers
    group by segment
)
select segment, segment_list
from customers_stats
order by segment
