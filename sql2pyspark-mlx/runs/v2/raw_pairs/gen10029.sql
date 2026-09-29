select
    c.country,
    quarter(c.signup_date) signup_quarter,
    round((100 * sum(case when a.account_type = 'LOAN' then 1 else 0 end)) / count(*), 1) pct_loan,
    min(a.opened_date) min_opened_date
from accounts a
left join customers c on a.customer_id = c.id
where a.status in ('CLOSED', 'FROZEN')
group by c.country, quarter(c.signup_date)
order by min_opened_date desc, signup_quarter desc;
