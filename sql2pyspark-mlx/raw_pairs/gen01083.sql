with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        quantity,
        last_restock,
        add_months(last_restock, 1) as last_restock_plus_months
    from inventory
    where quantity between 104 and 218
),
enriched as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.quantity,
        a.last_restock_plus_months,
        p.sqft,
        p.region,
        p.opened_date
    from filtered a
    left join stores p
        on a.store_id = p.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        store_id,
        sqft,
        region,
        ntile(3) over (partition by month(opened_date), store_id order by sqft desc, id) as sqft_bucket,
        avg(sqft) over (partition by store_id order by quantity, id rows between 6 preceding and 1 preceding) as sqft_avg_6_prior
    from enriched
    where opened_date <= '2024-10-01'
),
metrics_inventory as (
    -- Ranking and gap features
    select
        id,
        sqft,
        sqft_avg_6_prior,
        avg(sqft) over (partition by store_id, region order by sqft_avg_6_prior, id rows between 2 preceding and 1 preceding) as sqft_avg_2_prior,
        sum(sqft) over (partition by region order by sqft_avg_6_prior, id rows between unbounded preceding and current row) as running_sqft,
        ntile(10) over (partition by store_id order by sqft_bucket desc, id) as sqft_bucket_bucket,
        lead(sqft, 2, 0) over w_running as next_sqft,
        first_value(sqft, true) over (partition by store_id order by sqft_avg_6_prior, id rows between unbounded preceding and current row) as first_known_sqft
    from metrics
    window w_running as (partition by store_id, region order by sqft_bucket, id)
),
metrics_inventory_2 as (
    -- Ranking and gap features
    select
        *,
        avg(first_known_sqft) over w_running as last7_avg_first_known_sqft,
        max(first_known_sqft) over w_running as last7_max_first_known_sqft,
        min(first_known_sqft) over w_running as last7_min_first_known_sqft
    from metrics_inventory
    window w_running as (order by sqft, id rows between 6 preceding and current row)
)
select
    id,
    sqft_avg_6_prior,
    sqft_avg_2_prior,
    sqft_bucket_bucket,
    first_known_sqft,
    last7_max_first_known_sqft
from metrics_inventory_2
order by id desc nulls first;
