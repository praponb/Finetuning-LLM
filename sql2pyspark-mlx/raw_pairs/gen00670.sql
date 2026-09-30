with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        quantity,
        last_restock,
        date_format(last_restock, 'yyyy-MM-dd') last_restock_label
    from inventory
    where store_id = 5
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (order by quantity, id) quantity_bucket,
        avg(quantity) over w_ordered quantity_avg_3
    from cleaned
    window w_ordered as (order by last_restock, id rows between 3 preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select *, floor(quantity) quantity_whole
    from enriched
    where last_restock > date '2023-07-20'
),
valid as (
    -- Keep recent, valid rows
    select *, quantity + quantity_avg_3 quantity_plus_quantity_avg_3
    from base
    where quantity is not null
),
report as (
    select
        id,
        quantity,
        last_restock,
        quantity_bucket,
        quantity_avg_3,
        quantity_plus_quantity_avg_3
    from valid
    order by quantity desc nulls last, last_restock desc
)
select
    quantity,
    last_restock,
    quantity_bucket,
    quantity_avg_3,
    quantity_plus_quantity_avg_3
from report
