with valid as (
    -- Keep recent, valid rows
    select *, year(last_restock) as last_restock_year
    from inventory
    where quantity < 130
),
matched_keys as (
    -- Set intersection on keys
    select product_id, id
    from valid
    intersect all
    select product_id, id
    from inventory
    where last_restock > '2025-03-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.product_id = k.product_id and s.id = k.id
),
enriched as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by last_restock_year nulls last) as last_restock_year_dense_rank,
        rank() over (partition by store_id order by last_restock_year desc nulls first) as last_restock_year_rank,
        rank() over (partition by store_id order by last_restock_year desc nulls first) as last_restock_year_rank_2
    from reconciled
),
filtered as (
    -- Keep recent, valid rows
    select *, greatest(last_restock_year_rank, quantity) as greatest_last_restock_year_rank_quantity
    from enriched
    where last_restock > date '2023-07-14'
)
select
    s.id,
    s.store_id,
    s.last_restock_year,
    s.last_restock_year_dense_rank,
    s.last_restock_year_rank,
    s.greatest_last_restock_year_rank_quantity,
    (select avg(t2.units) from store_sales t2 where t2.store_id = s.store_id) as avg_store_sale_units
from filtered s
order by last_restock_year_dense_rank desc;
