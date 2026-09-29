with valid as (
    select
        *,
        round(units, 2) as units_rounded,
        date_sub(sale_date, 86) as sale_date_minus_86d
    from store_sales
    where sale_date >= date '2023-09-11'
)
select *
from (
    select
        id,
        product_id,
        units_rounded,
        revenue
    from valid
) src
unpivot (
    value for measure in (units_rounded, revenue)
)
