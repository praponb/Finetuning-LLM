select
    d.country,
    a.account_type,
    count(*) as rows_total,
    max(a.balance) as max_balance
from accounts a
inner join customers d on a.customer_id = d.id
group by grouping sets ((d.country, a.account_type), (d.country))
order by max_balance desc nulls first
