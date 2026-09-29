with metrics as (
    -- Ranking and gap features
    select
        id,
        order_id,
        shipped_date,
        cost,
        status,
        avg(cost) over (partition by carrier, order_id order by delivered_date, id rows between 5 preceding and 1 preceding) as cost_avg_5_prior,
        round(cost / sum(cost) over (partition by order_id, status), 4) as cost_share
    from shipments
),
metrics_shipments as (
    -- Ranking and gap features
    select
        *,
        sum(cost_share) over w_grp as running_cost_share,
        sum(cost_avg_5_prior) over (partition by date_trunc('WEEK', shipped_date), status order by cost, id rows between unbounded preceding and current row) as cumulative_cost_avg_5_prior,
        sum(cost_avg_5_prior) over (order by cost, id rows between unbounded preceding and current row) as cumulative_cost_avg_5_prior_2
    from metrics
    window w_grp as (order by cost_avg_5_prior, id rows between unbounded preceding and current row)
),
remaining_keys as (
    -- Set difference on keys
    select order_id
    from metrics_shipments
    except distinct
    select order_id
    from shipments
    where delivered_date >= '2024-09-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_shipments s
    inner join remaining_keys k on s.order_id = k.order_id
),
enriched as (
    -- Running and moving aggregates
    select *, cume_dist() over (order by cost_avg_5_prior nulls last) as cost_avg_5_prior_cume_dist
    from kept_rows
),
kept_keys as (
    -- Set intersection on keys
    select id, status
    from enriched
    intersect all
    select id, status
    from shipments
    where cost >= 91
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id and s.status = k.status
)
select
    s.id,
    s.order_id,
    s.cost,
    s.status,
    s.cost_avg_5_prior,
    s.cost_share,
    s.running_cost_share,
    s.cumulative_cost_avg_5_prior,
    s.cumulative_cost_avg_5_prior_2,
    case when status rlike '^(LOST|DELIVERED)$' then 'OTHER_ZONE' when status rlike '^(IN_TRANSIT|RETURNED)$' then 'GROUP_B' else 'CORE' end as status_zone,
    (select min(x.cost) from shipments x where x.order_id = s.order_id) as min_shipment_cost
from reconciled s
order by status_zone desc, cumulative_cost_avg_5_prior_2;
