with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        quantity,
        last_restock,
        date_add(last_restock, 52) as last_restock_plus_52d,
        date_sub(last_restock, 35) as last_restock_minus_35d
    from inventory
    where last_restock > '2025-03-04'
)
-- Ranking and gap features
select
    *,
    sum(quantity) over (order by last_restock_plus_52d, id rows between unbounded preceding and current row) as cumulative_quantity,
    avg(quantity) over (partition by store_id order by last_restock_plus_52d, id rows between 2 preceding and 1 preceding) as quantity_avg_2_prior
from filtered
