with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        ceil(quantity) as quantity_whole
    from order_items
    where product_id is not null
),
joined as (
    -- Attach products attributes
    select
        e.*,
        d.launch_date,
        d.stock_qty,
        d.price
    from base e
    inner join products d
        on e.product_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(stock_qty) over (partition by month(launch_date), product_id order by quantity, id rows between unbounded preceding and current row) as running_stock_qty,
        sum(quantity_whole) over (partition by order_id, product_id) as order_id_sum_quantity_whole,
        row_number() over (order by launch_date, id) as event_seq
    from joined
    where quantity_whole <= 20
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        quantity,
        quantity_whole,
        launch_date,
        running_stock_qty,
        event_seq,
        ntile(5) over (partition by order_id order by event_seq desc, id) as event_seq_bucket,
        avg(order_id_sum_quantity_whole) over (order by stock_qty, id rows between 3 preceding and 1 preceding) as order_id_sum_quantity_whole_avg_3_prior
    from scored
)
-- Window metrics over several different partitions
select
    id,
    order_id,
    quantity_whole,
    running_stock_qty,
    event_seq_bucket,
    order_id_sum_quantity_whole_avg_3_prior,
    max(event_seq) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_max_event_seq,
    min(event_seq) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_min_event_seq,
    avg(event_seq) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_avg_event_seq,
    row_number() over (partition by order_id order by quantity desc, id) as row_seq
from with_windows
