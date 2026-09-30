with valid as (
    -- Keep recent, valid rows
    select
        *,
        dayofweek(order_date) as order_date_dayofweek,
        date_add(order_date, 24) as order_date_plus_24d
    from orders
    where channel is distinct from 'partner'
      and amount <= 730
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(discount) over w_grp as cumulative_discount,
        first_value(shipping_fee, true) over w_part as first_known_shipping_fee
    from valid
    where amount < 1260
    window w_grp as (order by shipping_fee, id rows between unbounded preceding and current row), w_part as (order by discount, id rows between unbounded preceding and current row)
),
metrics_orders as (
    -- Ranking and gap features
    select
        *,
        corr(shipping_fee, discount) over (partition by status) as corr_shipping_fee_discount,
        percent_rank() over (partition by status order by order_date_dayofweek nulls last) as order_date_dayofweek_percent_rank,
        sum(first_known_shipping_fee) over (partition by channel order by first_known_shipping_fee, id rows between unbounded preceding and current row) as running_first_known_shipping_fee,
        ntile(3) over (partition by channel order by order_date_dayofweek desc, id) as order_date_dayofweek_bucket,
        last_value(order_date_dayofweek) ignore nulls over (partition by store_id order by order_date_plus_24d, id rows between unbounded preceding and current row) as last_known_order_date_dayofweek
    from metrics
),
enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        amount,
        discount,
        shipping_fee,
        corr_shipping_fee_discount,
        order_date_dayofweek_percent_rank,
        lead(shipping_fee, 1) over (partition by status, customer_id order by order_date_dayofweek_percent_rank, id) as next_shipping_fee,
        sum(last_known_order_date_dayofweek) over (partition by store_id order by cumulative_discount, id rows between unbounded preceding and current row) as cumulative_last_known_order_date_dayofweek
    from metrics_orders
)
-- Join a dimension and aggregate
select
    dim.segment,
    count(*) as rows_total,
    round(sum(a.corr_shipping_fee_discount), 2) as total_corr_shipping_fee_discount,
    min(a.next_shipping_fee) as min_next_shipping_fee
from enriched a
join customers dim on a.customer_id = dim.id
group by cube (dim.segment)
order by segment nulls first;
