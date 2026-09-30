select *
from (
    select brand, round((100 * sum(case when brand = 'Acme' then 1 else 0 end)) / count(*), 1) as pct_acme, max(launch_date) as last_launch_date
    from products
    group by brand
) as s
where pct_acme >= 79
order by 2
