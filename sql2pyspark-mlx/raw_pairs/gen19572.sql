with remaining_keys as (
    -- Set difference on keys
    select product_id
    from inventory
    except all
    select product_id
    from inventory
    where store_id in (1, 4, 5)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from inventory s
    join remaining_keys k on s.product_id = k.product_id
)
-- Aggregate with conditional metrics
select
    case when quantity >= 187 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
    date_trunc('quarter', last_restock) as quarter_start,
    avg(quantity) as avg_quantity,
    min(quantity) as min_quantity,
    count(*) as num_rows,
    round(avg(quantity), 2) as avg_quantity_2,
    sum(quantity) as total_quantity,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles
from kept_rows
group by 1, 2
