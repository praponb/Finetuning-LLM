with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(last_restock) as last_restock_month_end
    from inventory
    where last_restock >= date '2024-01-18'
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        p.price,
        p.product_name,
        p.launch_date
    from cleaned a
    left join products p
        on a.product_id = p.id
),
enriched_inventory as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_month_end,
        price,
        product_name,
        launch_date,
        round(price / sum(price) over (partition by store_id), 4) as price_share,
        cume_dist() over (partition by product_id, store_id order by quantity nulls first) as quantity_cume_dist,
        row_number() over (order by launch_date, id) as row_seq
    from enriched
),
joined as (
    -- Attach stores attributes
    select
        s.*,
        p2.region,
        p2.sqft,
        p2.opened_date
    from enriched_inventory s
    join stores p2
        on s.store_id = p2.id
)
select
    s.id,
    s.store_id,
    s.quantity,
    s.last_restock,
    s.product_name,
    s.quantity_cume_dist,
    s.sqft,
    (select min(x.quantity) from inventory x where x.store_id = s.store_id) as min_inventory_quantity
from joined s
order by quantity_cume_dist desc nulls last
