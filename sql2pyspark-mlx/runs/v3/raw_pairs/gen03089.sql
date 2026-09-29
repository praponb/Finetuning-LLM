with customer_summary as (
    select customer_id, sum(case when account_type = 'CREDIT' then 1 else 0 end) as credit_count
    from accounts
    group by customer_id
)
select c.segment, c.is_active, cs.credit_count
from customers c
left join customer_summary cs on c.id = cs.customer_id
order by credit_count desc
