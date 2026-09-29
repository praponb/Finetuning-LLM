with valid as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        date_format(last_restock, 'yyyy-MM') as last_restock_label
    from inventory
    where quantity = 209
),
latest as (
    select *, row_number() over (partition by store_id, product_id order by quantity desc, id) as rank_in_group
    from valid
    qualify rank_in_group = 1
),
with_dims as (
    select
        src.*,
        p.stock_qty,
        p.price
    from latest src
    left join products p
        on src.product_id = p.id
),
scored as (
    select
        *,
        lag(quantity) over (partition by store_id order by price, id) as prev_quantity,
        lag(price) over (partition by store_id order by price, id) as prev_price,
        lead(price) over (partition by store_id order by price, id) as next_price,
        row_number() over (partition by store_id order by price, id) as row_seq,
        lag(price, 1) over (partition by product_id order by last_restock, id) as prev_price_2
    from with_dims
    where store_id in (2, 5)
),
grouped as (
    select
        case when next_price >= 1160 then 'HIGH' else 'LOW' end as next_price_tier,
        percentile_approx(next_price, 0.5) as next_price_p50,
        count(distinct store_id) as distinct_store_ids,
        bit_or(quantity) as quantity_bit_or,
        round(avg(prev_quantity), 2) as avg_prev_quantity,
        percentile_approx(prev_price_2, array(0.25, 0.5, 0.75)) as prev_price_2_quartiles,
        percentile_approx(price, array(0.25, 0.5, 0.75)) as price_quartiles
    from scored
    group by 1
)
select
    next_price_tier,
    next_price_p50,
    distinct_store_ids,
    quantity_bit_or,
    prev_price_2_quartiles
from grouped
order by next_price_p50 nulls first;
