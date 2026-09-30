with cleaned as (
    -- Keep recent, valid rows
    select *, date_format(last_restock, 'yyyy-MM-dd') as last_restock_label
    from inventory
    where store_id in (4, 5, 6)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(quantity) over w_recent as next_quantity,
        lag(quantity) over w_recent as prev_quantity,
        row_number() over w_recent as row_seq
    from cleaned
    where product_id = 2
    window w_recent as (partition by store_id, product_id order by last_restock, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by store_id order by last_restock desc nulls first, id) as rn
    from scored
    qualify rn = 1
),
scored_inventory as (
    -- Ranking and gap features
    select
        id,
        product_id,
        last_restock_label,
        next_quantity,
        prev_quantity,
        rn,
        max(quantity) over w_running as last4_max_quantity,
        sum(quantity) over w_running as last4_sum_quantity,
        count(quantity) over w_running as last4_count_quantity
    from latest
    window w_running as (partition by store_id order by prev_quantity, id rows between 3 preceding and current row)
),
joined as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.next_quantity,
        src.last4_max_quantity,
        src.last4_sum_quantity,
        src.last4_count_quantity,
        p.product_name,
        p.launch_date
    from scored_inventory src
    left join products p
        on src.product_id = p.id
)
-- Multi-level aggregation
select
    case when next_quantity >= 182 then 'LARGE' else 'SMALL' end as next_quantity_tier,
    case when last4_count_quantity >= 13 then 'HIGH' else 'LOW' end as last4_count_quantity_tier,
    product_id,
    min(last4_count_quantity) as min_last4_count_quantity,
    min(next_quantity) as min_next_quantity,
    percentile_approx(last4_sum_quantity, 0.5) as last4_sum_quantity_p50,
    round(avg(last4_count_quantity), 2) as avg_last4_count_quantity,
    min(last4_sum_quantity) as min_last4_sum_quantity,
    count(distinct id) as distinct_ids
from joined
group by case when next_quantity >= 182 then 'LARGE' else 'SMALL' end, case when last4_count_quantity >= 13 then 'HIGH' else 'LOW' end, product_id
order by min_last4_sum_quantity desc nulls last;
