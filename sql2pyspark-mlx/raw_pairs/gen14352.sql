select
    p.status,
    a.merchant,
    count(*) as cnt,
    round(avg(a.amount), 2) as avg_amount
from transactions a
left join accounts p on a.account_id = p.id
group by cube (p.status, a.merchant)
order by avg_amount desc;
