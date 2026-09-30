select
    last_restock_year,
    product_id,
    max_quantity,
    quantity_count
from (
    select
        year(last_restock) last_restock_year,
        product_id,
        max(quantity) max_quantity,
        count(quantity) quantity_count
    from inventory
    group by year(last_restock), product_id
) x
where max_quantity >= 70
order by max_quantity desc, last_restock_year, product_id
limit 5
