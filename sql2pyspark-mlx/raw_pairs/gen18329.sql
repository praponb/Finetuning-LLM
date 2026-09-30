with with_dims as (
    -- Attach orders attributes
    select a.*, d.amount
    from shipments a
    join orders d
        on a.order_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        shipped_date,
        avg(amount) over (order by shipped_date, id rows between 2 preceding and 1 preceding) as amount_avg_2_prior,
        sum(cost) over w_recent as cumulative_cost,
        max(cost) over (partition by year(delivered_date), order_id) as delivered_date_year_max_cost,
        last_value(carrier, true) over (partition by status, order_id order by shipped_date, id rows between unbounded preceding and current row) as last_known_carrier,
        max(cost) over (partition by carrier) as carrier_max_cost
    from with_dims
    window w_recent as (partition by order_id order by delivered_date, id rows between unbounded preceding and current row)
),
with_windows_shipments as (
    -- Ranking and gap features
    select
        id,
        cumulative_cost,
        delivered_date_year_max_cost,
        last_known_carrier,
        avg(carrier_max_cost) over (order by amount_avg_2_prior, id rows between 6 preceding and 1 preceding) as carrier_max_cost_avg_6_prior,
        ntile(3) over (order by delivered_date_year_max_cost, id) as delivered_date_year_max_cost_bucket
    from with_windows
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        cumulative_cost,
        delivered_date_year_max_cost,
        delivered_date_year_max_cost_bucket,
        round(carrier_max_cost_avg_6_prior, 1) as carrier_max_cost_avg_6_prior_rounded
    from with_windows_shipments
    where delivered_date_year_max_cost < 48
)
select
    id,
    delivered_date_year_max_cost_bucket,
    carrier_max_cost_avg_6_prior_rounded
from cleaned
order by carrier_max_cost_avg_6_prior_rounded desc nulls last
