with filtered as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        round(quantity, 1) as quantity_rounded
    from inventory
    where last_restock >= date '2023-03-17'
      and product_id in (4, 5, 9)
),
metrics as (
    select
        id,
        store_id,
        product_id,
        sum(quantity_rounded) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as running_sum_quantity_rounded,
        count(quantity_rounded) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as running_count_quantity_rounded,
        max(quantity_rounded) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as running_max_quantity_rounded,
        avg(quantity_rounded) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as running_avg_quantity_rounded,
        ntile(3) over (partition by store_id, product_id order by quantity_rounded desc, id) as quantity_rounded_bucket
    from filtered
    where last_restock < '2024-02-05'
),
remaining_keys as (
    select product_id
    from metrics
    intersect
    select product_id
    from inventory
    where product_id in (2, 3, 8, 10)
),
verified as (
    select s.*
    from metrics s
    join remaining_keys k on s.product_id = k.product_id
),
base as (
    select
        id,
        store_id,
        running_sum_quantity_rounded,
        running_count_quantity_rounded,
        running_max_quantity_rounded,
        running_avg_quantity_rounded,
        quantity_rounded_bucket,
        cast(running_avg_quantity_rounded as int) as running_avg_quantity_rounded_int
    from verified
    where running_sum_quantity_rounded < 300
)
select
    case when running_count_quantity_rounded >= 16 then 'PREMIUM' else 'STANDARD' end as running_count_quantity_rounded_tier,
    store_id,
    min(running_count_quantity_rounded) as min_running_count_quantity_rounded,
    round(avg(running_count_quantity_rounded), 2) as avg_running_count_quantity_rounded,
    percentile_approx(running_sum_quantity_rounded, 0.95) as running_sum_quantity_rounded_p95,
    count(*) as cnt
from base
group by cube (case when running_count_quantity_rounded >= 16 then 'PREMIUM' else 'STANDARD' end, store_id)
having percentile_approx(running_sum_quantity_rounded, 0.95) >= 0;
