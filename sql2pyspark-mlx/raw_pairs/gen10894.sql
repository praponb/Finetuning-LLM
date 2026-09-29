with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        quantity,
        add_months(last_restock, 3) as last_restock_plus_months
    from inventory
    where last_restock >= to_date('2023-08-20')
      and quantity between 75 and 127
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        quantity,
        last_restock_plus_months,
        sum(quantity) over (order by quantity, id rows between unbounded preceding and current row) as running_quantity,
        first_value(quantity, true) over (partition by store_id order by last_restock_plus_months, id rows between unbounded preceding and current row) as first_known_quantity
    from filtered
    where store_id = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(quantity) over (partition by store_id order by last_restock_plus_months, id rows between unbounded preceding and current row) as running_count_quantity,
        min(quantity) over (partition by store_id order by last_restock_plus_months, id rows between unbounded preceding and current row) as running_min_quantity,
        avg(quantity) over (partition by store_id order by last_restock_plus_months, id rows between unbounded preceding and current row) as running_avg_quantity,
        avg(first_known_quantity) over (order by quantity, id rows between 6 preceding and 1 preceding) as first_known_quantity_avg_6_prior
    from with_windows
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        first_known_quantity,
        running_count_quantity,
        running_min_quantity,
        running_avg_quantity,
        first_known_quantity_avg_6_prior
    from (
        select *, row_number() over (partition by store_id order by running_quantity nulls last, id) as rn
        from enriched
    ) t
    where rn = 1
)
-- Aggregate with conditional metrics
select
    case when first_known_quantity >= 226 then 'LARGE' else 'SMALL' end as first_known_quantity_tier,
    case when running_min_quantity >= 256 then 'LARGE' else 'SMALL' end as running_min_quantity_tier,
    case when running_count_quantity >= 14 then 'LARGE' else 'SMALL' end as running_count_quantity_tier,
    count(distinct id) as distinct_ids,
    count(*) as cnt,
    count(*) as num_rows,
    approx_percentile(running_min_quantity, 0.5) as running_min_quantity_p50,
    count(*) as cnt_2
from latest
group by case when first_known_quantity >= 226 then 'LARGE' else 'SMALL' end, case when running_min_quantity >= 256 then 'LARGE' else 'SMALL' end, case when running_count_quantity >= 14 then 'LARGE' else 'SMALL' end
having count(*) > 1
order by cnt_2 nulls first
