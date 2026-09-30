with base as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        date_format(last_restock, 'yyyy-MM') as last_restock_label,
        datediff(current_date(), last_restock) / 365.25 as years_since_last_restock
    from inventory
    where last_restock >= to_date('2023-05-04')
),
deduped as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        last_restock_label
    from (
        select *, rank() over (partition by store_id order by quantity desc nulls first) as rank_in_group
        from base
    ) t
    where rank_in_group = 1
),
deduped_inventory as (
    select *, row_number() over (partition by store_id order by last_restock desc nulls last, id) as dedup_rank
    from deduped
    qualify dedup_rank = 1
),
deduped_inventory_2 as (
    select *, row_number() over (partition by store_id order by dedup_rank, id) as rank_in_group
    from deduped_inventory
    qualify rank_in_group = 1
),
valid as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        last_restock_label,
        dedup_rank,
        greatest(dedup_rank, quantity) as greatest_dedup_rank_quantity
    from deduped_inventory_2
    where store_id in (1, 3, 5, 6)
),
summary as (
    select
        date_trunc('year', last_restock) as year_start,
        percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
        round(avg(greatest_dedup_rank_quantity), 2) as avg_greatest_dedup_rank_quantity,
        approx_percentile(quantity, 0.5) as quantity_p50,
        count(*) as cnt
    from valid
    group by date_trunc('year', last_restock)
)
select
    year_start,
    quantity_quartiles,
    avg_greatest_dedup_rank_quantity,
    cnt
from summary
