with SupplierTotals as (
    select supplier_id, count(price) as price_count
    from products
    group by supplier_id
)
select s.country, s.supplier_name, S.price_count
from suppliers s
left join SupplierTotals S on s.id = S.supplier_id;
