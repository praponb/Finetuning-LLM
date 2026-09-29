with valid as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        quantity,
        last_restock,
        date_format(last_restock, 'MMM yyyy') last_restock_label,
        weekofyear(last_restock) last_restock_weekofyear
    from inventory
    where last_restock > to_date('2023-04-05')
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_hist event_seq,
        sum(quantity) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) running_quantity
    from valid
    where product_id = 1
    window w_hist as (partition by product_id order by last_restock_weekofyear, id)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by product_id order by last_restock_weekofyear desc nulls first, id) as latest_rank
    from metrics
    qualify latest_rank = 1
),
joined as (
    -- Attach products attributes
    select
        e.*,
        p.cost,
        p.price
    from deduped e
    left join products p
        on e.product_id = p.id
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by product_id order by event_seq nulls first) as rank_in_group
        from joined
    ) x
    where rank_in_group = 1
)
select
    id,
    product_id,
    running_quantity,
    latest_rank,
    cost,
    price,
    rank_in_group
from latest
order by cost desc nulls first;
