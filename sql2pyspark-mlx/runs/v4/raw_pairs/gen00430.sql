with prev_events as (
    select *, lag(txn_time) over (partition by merchant order by txn_time, id) as prev_txn_time
    from transactions
),
flagged as (
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval 30 days then 1 else 0 end as is_new_burst
    from prev_events
),
sessions_transactions as (
    select *, sum(is_new_burst) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    case when amount >= 4200 then 'HIGH' else 'LOW' end as amount_tier,
    count(*) filter (where txn_type = 'REFUND') as refund_rows,
    max(amount) filter (where txn_type = 'CREDIT') as credit_max_amount,
    percentile_approx(is_new_burst, 0.5) as is_new_burst_p50,
    sort_array(collect_set(merchant)) as merchant_list,
    avg(burst_id) filter (where txn_type = 'CREDIT') as credit_avg_burst_id,
    sum(case when merchant = 'Grab' then amount else 0 end) as grab_amount
from sessions_transactions
group by amount_tier
having percentile_approx(is_new_burst, 0.5) > 0
