with filtered_products as (
    select *
    from products
    where brand = 'Globex'
),
supplier_id_summary as (
    select supplier_id, count(case when category = 'Books' then 1 end) as books_count, min(launch_date) as first_launch_date
    from filtered_products
    group by supplier_id
)
select *
from supplier_id_summary
order by first_launch_date;
