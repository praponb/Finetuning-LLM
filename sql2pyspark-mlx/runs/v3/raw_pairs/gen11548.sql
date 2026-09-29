select
    a.account_type,
    year(a.opened_date) as opened_year,
    sum(case when t.merchant = 'Starbucks' then t.amount else 0 end) as starbucks_amount,
    max(t.amount) as max_amount
from transactions t
left outer join accounts a on t.account_id = a.id
group by a.account_type, year(a.opened_date)
order by account_type, starbucks_amount desc;
