with joined as (
    -- Attach orders attributes
    select
        e.*,
        p.channel,
        p.order_date
    from order_items e
    join orders p
        on e.order_id = p.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(unit_price) over (partition by channel order by order_date, id rows between 2 preceding and 1 preceding) as unit_price_avg_2_prior,
        avg(quantity) over (partition by order_id order by quantity, id rows between 6 preceding and 1 preceding) as quantity_avg_6_prior,
        lag(unit_price, 2) over (partition by order_id order by order_date, id) as prev_unit_price
    from joined
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        b.discount,
        b.amount,
        b.shipping_fee
    from with_windows s
    join orders b
        on s.order_id = b.id
),
enriched as (
    -- Attach products attributes
    select
        src.*,
        p.product_name,
        p.cost
    from with_dims src
    left join products p
        on src.product_id = p.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by order_date desc nulls first, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
)
-- Multi-level aggregation
select
    date_trunc('year', order_date) as year_start,
    avg(unit_price) filter (where channel = 'mobile') as mobile_avg_unit_price,
    round(avg(unit_price), 2) as avg_unit_price,
    avg(discount) filter (where channel = 'mobile') as mobile_avg_discount
from deduped
group by 1
order by year_start desc;
