with ranked_customers as (
    select *, dense_rank() over (partition by city order by credit_limit desc) as row_num
    from customers
)
select *
from ranked_customers
where row_num = 1
order by credit_limit desc
