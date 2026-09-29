with valid as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        quantity,
        last_restock,
        case when quantity >= 204 then 'High' when quantity >= 117 then 'Medium' else 'Low' end quantity_level
    from inventory
    where last_restock > date '2023-04-27'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(quantity, true) over (partition by product_id order by last_restock asc, id asc rows between unbounded preceding and current row) last_known_quantity,
        last_value(quantity, true) over (partition by product_id order by last_restock asc, id asc rows between unbounded preceding and current row) last_known_quantity_2
    from valid
    where quantity = 142
),
joined as (
    -- Attach products attributes
    select
        src.*,
        d.brand,
        d.price,
        d.product_name
    from metrics src
    join products d
        on src.product_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_id,
        quantity,
        last_restock,
        last_known_quantity_2,
        price,
        sum(last_known_quantity) over (partition by product_id order by last_known_quantity asc, id asc rows between unbounded preceding and current row) running_last_known_quantity,
        avg(last_known_quantity_2) over (partition by product_id order by last_known_quantity asc, id asc rows between 5 preceding and current row) last_known_quantity_2_avg_5
    from joined
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(last_known_quantity_2) over w_hist running_max_last_known_quantity_2,
        sum(last_known_quantity_2) over w_hist running_sum_last_known_quantity_2,
        count(last_known_quantity_2) over w_hist running_count_last_known_quantity_2
    from scored
    window w_hist as (partition by product_id order by last_restock asc, id asc rows between unbounded preceding and current row)
),
valid_inventory as (
    -- Keep recent, valid rows
    select
        *,
        cast(running_sum_last_known_quantity_2 as int) running_sum_last_known_quantity_2_int,
        case when running_last_known_quantity >= 1074 then 'High' when running_last_known_quantity >= 577 then 'Medium' else 'Low' end running_last_known_quantity_band
    from with_windows
    where last_restock >= date '2023-06-23'
      and last_known_quantity_2_avg_5 != 229
)
-- Grouped metrics
select
    date_trunc('year', last_restock) as year_start,
    avg(last_known_quantity_2_avg_5) as avg_last_known_quantity_2_avg_5,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    bit_or(quantity) as quantity_bit_or
from valid_inventory
group by year_start;
