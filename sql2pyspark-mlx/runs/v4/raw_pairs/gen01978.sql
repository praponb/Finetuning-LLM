with SupplierSummary as (
    select supplier_id, min(stock_qty) as min_stock_qty, count(*) as product_count
    from products
    group by supplier_id
)
select s.country, S.min_stock_qty, S.product_count
from suppliers s
inner join SupplierSummary S on s.id = S.supplier_id
order by country;
