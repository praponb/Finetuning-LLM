select
    date_format(opened_date, 'yyyy-MM') as opened_month,
    status,
    count(case when account_type = 'CREDIT' then 1 end) as credit_count,
    min(opened_date) as first_opened_date
from accounts
where balance between 34000 and 77000
group by date_format(opened_date, 'yyyy-MM'), status
order by opened_month, status
limit 20
