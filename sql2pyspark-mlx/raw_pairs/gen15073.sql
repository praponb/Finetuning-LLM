select
    s.*,
    latest.store_sale_sale_date,
    latest.units
from products s
left join lateral (
        select c.sale_date as store_sale_sale_date, c.units
        from store_sales c
        where c.product_id = s.id
        order by c.revenue desc, c.id desc
        limit 1
    ) latest on true
