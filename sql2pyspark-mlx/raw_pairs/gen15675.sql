with base as (
    select *, coalesce(quantity, 0) quantity_filled
    from inventory
    where last_restock >= date '2023-04-04'
),
joined as (
    select
        a.id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.quantity_filled,
        b.product_name
    from base a
    left join products b
        on a.product_id = b.id
),
ranked as (
    select *
    from (
        select *, rank() over (partition by product_id order by last_restock desc nulls last) as rank_in_group
        from joined
    ) t
    where rank_in_group = 1
),
scored as (
    select
        id,
        product_id,
        quantity,
        product_name,
        rank_in_group,
        min(quantity_filled) over w_grp running_min_quantity_filled,
        sum(quantity_filled) over w_grp running_sum_quantity_filled,
        max(quantity_filled) over w_grp running_max_quantity_filled
    from ranked
    where rank_in_group <> 1
    window w_grp as (partition by product_id order by rank_in_group, id rows between unbounded preceding and current row)
)
select
    p.brand,
    p.category,
    count(*) as rows_total,
    min(s.running_min_quantity_filled) as min_running_min_quantity_filled,
    max(s.running_sum_quantity_filled) as max_running_sum_quantity_filled,
    grouping_id() as level_id
from scored s
left join products p on s.product_id = p.id
group by cube (p.brand, p.category)
