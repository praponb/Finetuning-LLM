select
    i.last_restock,
    i.quantity,
    i.store_id,
    ss.units,
    year(ss.sale_date) as sale_date_year
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.sale_date <= '2024-08-01' and ss.units < 16
order by sale_date_year desc, units;
