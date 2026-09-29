with supplier_summary as (
    select supplier_id, max(price) - min(price) price_range, sum(case when brand = 'Stark' then 1 else 0 end) stark_count
    from products
    where launch_date between '2024-03-19' and '2024-09-18'
    group by supplier_id
)
select
    suppliers.id,
    suppliers.rating,
    supplier_summary.price_range,
    supplier_summary.stark_count
from suppliers
left join supplier_summary on suppliers.id = supplier_summary.supplier_id
where supplier_summary.price_range >= 77;
