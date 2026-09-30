select *
from (
    select case when price >= 910 then 'high' when price >= 250 then 'mid' else 'low' end as price_bucket, date_format(launch_date, 'yyyy-MM') as launch_month, count(*) as product_count
    from products
    where product_name like '%Chair%'
    group by case when price >= 910 then 'high' when price >= 250 then 'mid' else 'low' end, date_format(launch_date, 'yyyy-MM')
) as s
where product_count < 8;
