with supplier_summary as (
    select supplier_id, count(*) as total_count, min(launch_date) as min_launch_date
    from products
    group by supplier_id
)
select
    suppliers.id,
    suppliers.supplier_name,
    supplier_summary.total_count,
    supplier_summary.min_launch_date
from suppliers
left join supplier_summary on suppliers.id = supplier_summary.supplier_id
where supplier_summary.total_count > 9;
