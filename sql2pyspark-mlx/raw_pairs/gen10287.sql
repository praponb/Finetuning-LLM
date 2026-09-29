with valid as (
    -- Filter the raw rows and derive helper columns
    select *, least(unit_price, quantity) as least_unit_price_quantity
    from order_items
    where unit_price > 300
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(unit_price) over (partition by order_id, product_id) as order_id_stddev_samp_unit_price,
        avg(unit_price) over (partition by order_id order by least_unit_price_quantity, id rows between 3 preceding and 1 preceding) as unit_price_avg_3_prior
    from valid
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id order by unit_price desc nulls last, id) as rank_in_group
        from with_windows
    ) x
    where rank_in_group = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        max(unit_price_avg_3_prior) over w_running as running_max_unit_price_avg_3_prior,
        count(unit_price_avg_3_prior) over w_running as running_count_unit_price_avg_3_prior,
        sum(unit_price_avg_3_prior) over w_running as running_sum_unit_price_avg_3_prior,
        row_number() over w_part as row_seq
    from ranked
    window w_running as (partition by order_id order by rank_in_group, id rows between unbounded preceding and current row), w_part as (partition by product_id, order_id order by unit_price_avg_3_prior desc, id)
),
valid_order_items as (
    -- Keep recent, valid rows
    select *, running_count_unit_price_avg_3_prior + unit_price as running_count_unit_price_avg_3_prior_plus_unit_price
    from enriched
    where product_id is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        quantity,
        unit_price,
        least_unit_price_quantity,
        unit_price_avg_3_prior,
        rank_in_group,
        running_max_unit_price_avg_3_prior,
        running_count_unit_price_avg_3_prior,
        running_sum_unit_price_avg_3_prior,
        running_count_unit_price_avg_3_prior_plus_unit_price,
        round(quantity, 2) as quantity_rounded
    from valid_order_items
    where quantity > 7
),
ranked_order_items as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by unit_price_avg_3_prior desc, id) as latest_rank
        from cleaned
    ) t
    where latest_rank = 1
)
-- Grouped metrics
select
    case when running_sum_unit_price_avg_3_prior >= 1560 then 'LARGE' else 'SMALL' end as running_sum_unit_price_avg_3_prior_tier,
    approx_percentile(quantity_rounded, array(0.25, 0.5, 0.75)) as quantity_rounded_quartiles,
    avg(quantity) as avg_quantity,
    count(*) as event_count,
    bit_and(quantity) as quantity_bit_and
from ranked_order_items
group by case when running_sum_unit_price_avg_3_prior >= 1560 then 'LARGE' else 'SMALL' end
