with with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        row_number() over (partition by carrier order by delivered_date, id) event_seq,
        round(cost / sum(cost) over (partition by order_id, status), 4) cost_share,
        sum(cost) over (partition by status) status_sum_cost,
        round(cost / sum(cost) over (partition by order_id), 4) cost_share_2,
        avg(cost) over (partition by status order by shipped_date, id rows between 6 preceding and current row) cost_avg_6
    from shipments
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.event_seq,
    s.cost_share,
    (select sum(t2.amount) from payments t2 where t2.order_id = s.order_id) as sum_payment_amount
from with_windows s
order by cost desc;
