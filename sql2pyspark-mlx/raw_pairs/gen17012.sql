with metrics as (
    -- Ranking and gap features
    select
        id,
        order_id,
        row_number() over (partition by product_id order by quantity, id) as row_seq,
        corr(quantity, unit_price) over (partition by product_id, order_id) as corr_quantity_unit_price
    from order_items
    where product_id = 40
),
base as (
    -- Basic cleansing and defaults
    select *, row_seq - corr_quantity_unit_price as row_seq_minus_corr_quantity_unit_price
    from metrics
    where order_id = 28
),
enriched as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.row_seq,
        b.shipping_fee
    from base e
    left join orders b
        on e.order_id = b.id
),
valid as (
    -- Basic cleansing and defaults
    select *, shipping_fee - row_seq as shipping_fee_minus_row_seq
    from enriched
    where row_seq = 8
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        row_seq,
        shipping_fee,
        shipping_fee_minus_row_seq,
        max(shipping_fee) over w_grp as order_id_max_shipping_fee,
        last_value(row_seq) ignore nulls over w_recent as last_known_row_seq,
        stddev_samp(shipping_fee_minus_row_seq) over w_grp as order_id_stddev_samp_shipping_fee_minus_row_seq
    from valid
    window w_grp as (partition by order_id), w_recent as (partition by order_id order by row_seq, id rows between unbounded preceding and current row)
),
report as (
    select
        id,
        shipping_fee,
        order_id_max_shipping_fee,
        order_id_stddev_samp_shipping_fee_minus_row_seq
    from with_windows
    order by order_id_stddev_samp_shipping_fee_minus_row_seq desc nulls last
)
select
    id,
    shipping_fee,
    order_id_stddev_samp_shipping_fee_minus_row_seq
from report
order by id desc;
