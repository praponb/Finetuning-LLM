with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        unit_price * quantity as line_total,
        quantity * unit_price as line_total_2
    from order_items
    where product_id = 5
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        unit_price,
        line_total
    from (
        select *, rank() over (partition by order_id, product_id order by line_total desc nulls first) as latest_rank
        from cleaned
    ) t
    where latest_rank = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, floor(line_total) as line_total_whole
    from deduped
    where unit_price >= 250
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over (order by unit_price desc, id) as seq_no,
        dense_rank() over (order by unit_price nulls last) as unit_price_dense_rank
    from base
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        unit_price,
        line_total,
        line_total_whole,
        seq_no,
        unit_price_dense_rank,
        ceil(line_total_whole) as line_total_whole_whole
    from enriched
    where line_total <= 23
),
remaining_keys as (
    -- Set difference on keys
    select id
    from filtered
    except all
    select id
    from order_items
    where unit_price >= 350
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.id = k.id
),
kept_keys as (
    -- Set difference on keys
    select id
    from verified
    except
    select id
    from order_items
    where quantity between 5 and 6
),
verified_order_items as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join kept_keys k on s.id = k.id
)
select
    line_total,
    line_total_whole,
    seq_no,
    unit_price_dense_rank,
    line_total_whole_whole
from verified_order_items
order by line_total_whole_whole desc;
