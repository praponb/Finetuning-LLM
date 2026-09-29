with filtered_customers as (
    select *
    from customers
    where credit_limit >= 7500 and city in ('Berlin', 'Bangkok', 'Chiang Mai')
),
signup_month_summary as (
    select month(signup_date) as signup_month, sum(case when country = 'JP' then age else 0 end) as jp_age
    from filtered_customers
    group by month(signup_date)
)
select signup_month, jp_age
from signup_month_summary
where jp_age >= 63
order by signup_month desc, 2 desc;
