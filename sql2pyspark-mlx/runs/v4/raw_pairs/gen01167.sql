with kept_keys as (
    -- Set difference on keys
    select id
    from inventory
    except all
    select id
    from inventory
    where quantity <= 94
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from inventory s
    inner join kept_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    product_id,
    store_id,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles_2,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles_3
from kept_rows
group by product_id, store_id;
