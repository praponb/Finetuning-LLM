with product_summary as (
    select product_id, max(quantity) - min(quantity) as quantity_range, count(*) as row_count
    from inventory
    group by product_id
)
select p.supplier_id, ps.quantity_range, ps.row_count
from products p
inner join product_summary ps on p.id = ps.product_id
where ps.quantity_range > 53;
