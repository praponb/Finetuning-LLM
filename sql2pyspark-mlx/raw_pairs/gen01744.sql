with customer_stats as (
    select customer_id, sort_array(collect_set(account_type)) as account_type_list, count(balance) as balance_count
    from accounts
    group by customer_id
)
select c.email, cs.account_type_list, cs.balance_count
from customers c
left outer join customer_stats cs on c.id = cs.customer_id
where cs.balance_count > 8
order by cs.balance_count desc
