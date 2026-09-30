with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        last_restock,
        add_months(last_restock, 1) as last_restock_plus_months
    from inventory
    where last_restock > date '2023-09-27'
)
-- Attach products attributes
select
    a.id,
    a.product_id,
    a.last_restock,
    d.stock_qty
from base a
left join products d
    on a.product_id = d.id;
