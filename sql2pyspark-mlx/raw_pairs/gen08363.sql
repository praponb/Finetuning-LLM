with filtered as (
    -- Keep recent, valid rows
    select *, coalesce(quantity, 0) quantity_filled
    from inventory
    where product_id in (2, 4, 6, 7)
),
with_dims as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.quantity,
        e.last_restock,
        e.quantity_filled,
        b.sqft,
        b.region,
        b.store_name
    from filtered e
    left join stores b
        on e.store_id = b.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lag(sqft) over w_running prev_sqft,
        lag(quantity_filled) over w_running prev_quantity_filled,
        lead(quantity_filled) over w_running next_quantity_filled,
        row_number() over w_running seq_no
    from with_dims
    window w_running as (partition by region order by sqft, id)
),
kept_keys as (
    -- Set intersection on keys
    select store_id, id
    from with_windows
    intersect
    select store_id, id
    from inventory
    where product_id in (1, 6, 8)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.store_id = k.store_id and s.id = k.id
),
matched_keys as (
    -- Set intersection on keys
    select store_id, id
    from verified
    intersect all
    select store_id, id
    from inventory
    where last_restock >= '2023-04-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join matched_keys k on s.store_id = k.store_id and s.id = k.id
)
-- Aggregate with conditional metrics
select
    case when quantity >= 266 then 'LARGE' else 'SMALL' end as quantity_tier,
    store_id,
    count(*) filter (where region = 'South') as south_rows,
    approx_percentile(quantity_filled, 0.95) as quantity_filled_p95,
    count(distinct id) as distinct_ids,
    percentile_approx(quantity_filled, array(0.25, 0.5, 0.75)) as quantity_filled_quartiles,
    sum(case when region = 'South' then seq_no else 0 end) as south_seq_no,
    count(*) filter (where region = 'West') as west_rows
from kept_rows
group by case when quantity >= 266 then 'LARGE' else 'SMALL' end, store_id
having south_seq_no > 0
order by west_rows
