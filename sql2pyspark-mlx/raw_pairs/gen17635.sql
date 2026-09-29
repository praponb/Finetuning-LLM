with scored as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        last_restock,
        avg(quantity) over (partition by product_id, store_id order by quantity, id rows between unbounded preceding and current row) running_avg_quantity,
        max(quantity) over (partition by product_id, store_id order by quantity, id rows between unbounded preceding and current row) running_max_quantity,
        count(quantity) over (partition by product_id, store_id order by quantity, id rows between unbounded preceding and current row) running_count_quantity,
        min(quantity) over (partition by product_id, store_id order by quantity, id rows between unbounded preceding and current row) running_min_quantity,
        sum(quantity) over (partition by year(last_restock), product_id order by quantity, id rows between unbounded preceding and current row) running_quantity
    from inventory
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by month(last_restock), product_id order by running_avg_quantity desc, id) row_seq,
        sum(running_max_quantity) over w_ordered cumulative_running_max_quantity,
        ntile(5) over w_hist running_quantity_bucket,
        sum(running_min_quantity) over (partition by product_id order by running_count_quantity, id rows between unbounded preceding and current row) cumulative_running_min_quantity,
        avg(running_max_quantity) over (partition by store_id order by running_avg_quantity, id rows between 2 preceding and 1 preceding) running_max_quantity_avg_2_prior
    from scored
    where store_id in (2, 4)
    window w_ordered as (partition by store_id order by running_quantity, id rows between unbounded preceding and current row), w_hist as (partition by month(last_restock), product_id order by running_quantity, id)
),
with_dims as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.last_restock,
        e.running_avg_quantity,
        e.running_count_quantity,
        e.row_seq,
        e.cumulative_running_min_quantity,
        b.city
    from metrics e
    left join stores b
        on e.store_id = b.id
),
metrics_inventory as (
    -- Ranking and gap features
    select
        id,
        store_id,
        running_avg_quantity,
        row_seq,
        cumulative_running_min_quantity,
        city,
        avg(row_seq) over (partition by date_trunc('WEEK', last_restock), store_id order by cumulative_running_min_quantity, id rows between 3 preceding and current row) row_seq_avg_3,
        row_number() over (partition by city order by last_restock desc, id) event_seq,
        sum(running_count_quantity) over (partition by city, store_id order by running_count_quantity, id rows between unbounded preceding and current row) cumulative_running_count_quantity
    from with_dims
),
joined as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.cumulative_running_min_quantity,
        src.city,
        src.row_seq_avg_3,
        src.cumulative_running_count_quantity,
        s.opened_date
    from metrics_inventory src
    left join stores s
        on src.store_id = s.id
)
-- Window metrics over several different partitions
select
    id,
    store_id,
    cumulative_running_min_quantity,
    city,
    row_seq_avg_3,
    cumulative_running_count_quantity,
    count(cumulative_running_count_quantity) over w_part running_count_cumulative_running_count_quantity,
    sum(cumulative_running_count_quantity) over w_part running_sum_cumulative_running_count_quantity,
    avg(cumulative_running_count_quantity) over w_part running_avg_cumulative_running_count_quantity,
    avg(cumulative_running_min_quantity) over w_grp cumulative_running_min_quantity_avg_3
from joined
window w_part as (partition by store_id, city order by opened_date, id rows between unbounded preceding and current row), w_grp as (partition by store_id order by opened_date, id rows between 3 preceding and current row);
