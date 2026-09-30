with account_totals as (
    select account_id, count(*) as cnt, count(*) as num_transactions
    from transactions
    where merchant is not null
    group by account_id
)
select
    a.id,
    a.status,
    at.cnt,
    at.num_transactions
from accounts a
left outer join account_totals at on a.id = at.account_id
where at.cnt >= 7
order by a.status desc;
