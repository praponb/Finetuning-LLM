with numbered as (
    select
        id,
        store_id,
        units,
        product_id,
        row_number() over (partition by store_id order by units desc, id) row_num
    from store_sales
)
select *
from numbered
where row_num <= 3
