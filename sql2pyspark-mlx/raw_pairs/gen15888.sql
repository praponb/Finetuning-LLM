with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        status,
        channel,
        shipping_fee,
        shipping_fee + discount as shipping_fee_plus_discount,
        year(order_date) as order_date_year
    from orders
    where channel is distinct from 'web'
      and order_date >= to_date('2023-05-19')
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by shipping_fee_plus_discount nulls first, id) as rn
    from base
    qualify rn = 1
),
with_dims as (
    -- Attach stores attributes
    select src.*, b.opened_date
    from deduped src
    join stores b
        on src.store_id <=> b.id
    where b.city not in ('Berlin', 'New York', 'Tokyo')
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(shipping_fee_plus_discount) over (partition by store_id, channel order by opened_date, id) as next_shipping_fee_plus_discount,
        lag(shipping_fee_plus_discount) over (partition by store_id, channel order by opened_date, id) as prev_shipping_fee_plus_discount,
        lag(order_date_year) over (partition by store_id, channel order by opened_date, id) as prev_order_date_year,
        lead(order_date_year) over (partition by store_id, channel order by opened_date, id) as next_order_date_year,
        row_number() over (partition by store_id, channel order by opened_date, id) as row_seq,
        first_value(shipping_fee) ignore nulls over (order by shipping_fee_plus_discount, id rows between unbounded preceding and current row) as first_known_shipping_fee
    from with_dims
)
select
    s.id,
    s.store_id,
    s.shipping_fee,
    s.shipping_fee_plus_discount,
    s.order_date_year,
    s.rn,
    s.opened_date,
    s.next_shipping_fee_plus_discount,
    s.prev_shipping_fee_plus_discount,
    (select avg(x.units) from store_sales x where x.store_id = s.store_id) as avg_store_sale_units
from enriched s
order by prev_shipping_fee_plus_discount
