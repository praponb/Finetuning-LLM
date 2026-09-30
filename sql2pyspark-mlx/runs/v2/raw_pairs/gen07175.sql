with valid_customers as (
    select *, date_format(signup_date, 'yyyy-MM-dd') signup_date_label
    from customers
    where name like '%Emma%' and is_active = true
),
summary as (
    select is_active, max(credit_limit) max_credit_limit, sum(age) total_age
    from valid_customers
    group by is_active
)
select is_active, max_credit_limit, total_age
from summary
where max_credit_limit > 15200
order by max_credit_limit desc, total_age desc;
