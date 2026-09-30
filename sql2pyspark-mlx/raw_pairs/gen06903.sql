with numbered as (
    select *, row_number() over (partition by product_id order by sale_date, id) as pos
    from store_sales
)
select *
from numbered
where pos <= 3
order by product_id
