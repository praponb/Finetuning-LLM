with numbered as (
    select
        id,
        store_id,
        sale_date,
        revenue,
        row_number() over (partition by store_id order by sale_date desc, id) as rnk
    from store_sales
    where store_id in (1, 2, 3, 5)
)
select *
from numbered
where rnk <= 2
order by store_id asc;
