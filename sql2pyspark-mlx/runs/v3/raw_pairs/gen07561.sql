with recent_customers as (
    select *
    from customers
    where not (not is_active and name not like 'Kan%' and city in ('New York', 'Chiang Mai', 'Berlin'))
),
summary as (
    select segment, sort_array(collect_set(country)) country_list, max(credit_limit) max_credit_limit
    from recent_customers
    group by segment
)
select segment, country_list, max_credit_limit
from summary;
