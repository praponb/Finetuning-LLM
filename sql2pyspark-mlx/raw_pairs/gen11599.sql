with ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by quantity desc, id) as row_num
    from inventory
    qualify row_num = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        ceil(quantity) as quantity_whole
    from ranked
    where last_restock >= date '2023-09-20'
      and product_id in (3, 4, 5, 6)
),
kept_keys as (
    -- Set difference on keys
    select id
    from filtered
    except
    select id
    from inventory
    where product_id in (7, 9, 10)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join kept_keys k on s.id = k.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        floor(quantity_whole) as quantity_whole_whole,
        quantity_whole + quantity as quantity_whole_plus_quantity
    from verified
    where last_restock >= date '2023-09-11'
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        p.cost,
        p.stock_qty,
        p.category
    from base a
    left join products p
        on a.product_id = p.id
)
-- Aggregate with conditional metrics
select
    category,
    case when quantity_whole_plus_quantity >= 85 then 'PREMIUM' else 'STANDARD' end as quantity_whole_plus_quantity_tier,
    round(sum(quantity_whole_whole) / nullif(count(case when category = 'Home' then 1 end), 0), 2) as quantity_whole_whole_per_home,
    count(*) as num_rows,
    count(*) filter (where category = 'Fashion') as fashion_rows,
    stddev_samp(quantity) as quantity_stddev
from enriched
group by category, case when quantity_whole_plus_quantity >= 85 then 'PREMIUM' else 'STANDARD' end
having num_rows >= 1;
