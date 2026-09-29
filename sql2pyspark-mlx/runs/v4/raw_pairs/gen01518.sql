with scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        percent_rank() over (partition by product_id order by quantity desc) quantity_percent_rank,
        avg(quantity) over (partition by product_id order by quantity asc, id asc rows between 5 preceding and current row) quantity_avg_5,
        avg(quantity) over (order by quantity asc, id asc rows between 2 preceding and 1 preceding) quantity_avg_2_prior
    from inventory
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity desc, id asc) as dedup_rank
        from scored
    ) ranked
    where dedup_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(quantity_avg_5) over (partition by product_id, store_id) product_id_min_quantity_avg_5,
        cume_dist() over (partition by store_id, product_id order by quantity_avg_5 asc) quantity_avg_5_cume_dist
    from deduped
),
metrics_inventory as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_percent_rank,
        quantity_avg_5,
        quantity_avg_2_prior,
        dedup_rank,
        product_id_min_quantity_avg_5,
        quantity_avg_5_cume_dist,
        avg(quantity_avg_2_prior) over (partition by store_id order by quantity_avg_5 asc, id asc rows between 3 preceding and current row) quantity_avg_2_prior_avg_3,
        ntile(5) over (partition by store_id order by quantity_avg_5_cume_dist asc, id asc) quantity_avg_5_cume_dist_bucket,
        sum(quantity) over (order by quantity asc, id asc rows between unbounded preceding and current row) running_quantity
    from metrics
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        last_restock,
        quantity_percent_rank,
        quantity_avg_5,
        quantity_avg_2_prior,
        dedup_rank,
        product_id_min_quantity_avg_5,
        quantity_avg_5_cume_dist,
        quantity_avg_2_prior_avg_3,
        quantity_avg_5_cume_dist_bucket,
        running_quantity,
        cast(quantity_avg_5_cume_dist as int) quantity_avg_5_cume_dist_int,
        quantity_avg_5_cume_dist / nullif(quantity_avg_5, 0) quantity_avg_5_cume_dist_per_quantity_avg_5
    from metrics_inventory
    where last_restock > date '2024-02-10'
),
scored_inventory as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        last_restock,
        quantity_percent_rank,
        quantity_avg_5,
        quantity_avg_2_prior,
        dedup_rank,
        product_id_min_quantity_avg_5,
        quantity_avg_5_cume_dist,
        quantity_avg_2_prior_avg_3,
        quantity_avg_5_cume_dist_bucket,
        running_quantity,
        quantity_avg_5_cume_dist_int,
        quantity_avg_5_cume_dist_per_quantity_avg_5,
        first_value(quantity_avg_2_prior) ignore nulls over (partition by product_id order by last_restock asc, id asc rows between unbounded preceding and current row) first_known_quantity_avg_2_prior,
        row_number() over w_recent event_seq
    from cleaned
    window w_recent as (order by product_id_min_quantity_avg_5 desc, id asc)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from scored_inventory
    qualify row_number() over (partition by store_id, product_id order by dedup_rank desc, id asc) = 1
)
-- Grouped metrics
select
    store_id,
    count(distinct store_id) as distinct_store_ids,
    approx_percentile(quantity_avg_5_cume_dist, array(0.25, 0.5, 0.75)) as quantity_avg_5_cume_dist_quartiles,
    percentile_approx(quantity_percent_rank, array(0.25, 0.5, 0.75)) as quantity_percent_rank_quartiles,
    sum(quantity_percent_rank) as total_quantity_percent_rank,
    approx_percentile(quantity_avg_5_cume_dist_bucket, array(0.25, 0.5, 0.75)) as quantity_avg_5_cume_dist_bucket_quartiles,
    round(avg(quantity_percent_rank), 2) as avg_quantity_percent_rank
from latest
group by store_id
order by distinct_store_ids asc
