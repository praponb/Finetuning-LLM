with src as (
    select *
    from subscriptions
    where customer_id = 36 and plan != 'premium'
),
summary as (
    select
        plan,
        year(start_date) start_year,
        sort_array(collect_set(status)) status_list,
        sort_array(collect_set(plan)) plan_list
    from src
    group by plan, year(start_date)
)
select *
from summary
order by plan desc;
