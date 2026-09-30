with customer_stats as (
    select customer_id, sum(case when status = 'OPEN' then 1 else 0 end) as open_count, min(balance) as min_balance
    from accounts
    where status not in ('FROZEN', 'OPEN')
    group by customer_id
)
select
    c.id,
    c.city,
    cs.open_count,
    cs.min_balance
from customers c
left join customer_stats cs on c.id = cs.customer_id
where cs.open_count >= 45
order by c.id asc;
