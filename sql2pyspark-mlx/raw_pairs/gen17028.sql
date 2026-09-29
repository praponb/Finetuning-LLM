with enriched as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        count(quantity) over w_recent as running_count_quantity,
        max(quantity) over w_recent as running_max_quantity,
        sum(quantity) over w_recent as running_sum_quantity,
        avg(quantity) over (partition by store_id order by quantity, id rows between 3 preceding and current row) as quantity_avg_3
    from inventory
    window w_recent as (partition by store_id, product_id order by quantity, id rows between unbounded preceding and current row)
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, quantity_avg_3 / nullif(running_sum_quantity, 0) as quantity_avg_3_per_running_sum_quantity
    from enriched
    where quantity_avg_3 between 166 and 204
),
kept_keys as (
    -- Set intersection on keys
    select id
    from base
    intersect all
    select id
    from inventory
    where store_id in (3, 6)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.id = k.id
),
cleaned as (
    -- Basic cleansing and defaults
    select *, cast(quantity_avg_3_per_running_sum_quantity as int) as quantity_avg_3_per_running_sum_quantity_int
    from kept_rows
    where running_sum_quantity > 720
),
enriched_inventory as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over w_grp as quantity_avg_3_dense_rank,
        percent_rank() over (order by quantity_avg_3) as quantity_avg_3_percent_rank,
        percent_rank() over (order by running_sum_quantity desc nulls last) as running_sum_quantity_percent_rank,
        percent_rank() over (order by running_count_quantity) as running_count_quantity_percent_rank
    from cleaned
    window w_grp as (order by quantity_avg_3 desc nulls first)
)
-- Ranking and gap features
select
    running_count_quantity,
    running_max_quantity,
    quantity_avg_3_per_running_sum_quantity_int,
    quantity_avg_3_dense_rank,
    running_sum_quantity_percent_rank,
    running_count_quantity_percent_rank,
    percent_rank() over (order by running_count_quantity_percent_rank desc nulls first) as running_count_quantity_percent_rank_percent_rank,
    percent_rank() over (order by running_count_quantity desc nulls last) as running_count_quantity_percent_rank_2,
    cume_dist() over (order by running_count_quantity nulls first) as running_count_quantity_cume_dist,
    cume_dist() over (order by running_max_quantity desc nulls first) as running_max_quantity_cume_dist
from enriched_inventory;
