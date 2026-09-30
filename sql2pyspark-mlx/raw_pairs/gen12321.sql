with cleaned as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        length(status) as status_length,
        coalesce(status, 'N/A') as status_filled
    from accounts
    where opened_date > date '2023-04-14'
      and status is not null
),
scored as (
    select
        *,
        sum(balance) over (partition by customer_id order by status_length, id rows between unbounded preceding and current row) as cumulative_balance,
        avg(status_length) over (partition by account_type order by opened_date, id rows between 6 preceding and current row) as status_length_avg_6,
        avg(status_length) over w_grp as status_length_avg_2
    from cleaned
    where customer_id in (22, 30, 39)
    window w_grp as (partition by status order by status_length, id rows between 2 preceding and current row)
),
with_latest as (
    select
        s.*,
        x.merchant,
        x.transaction_amount
    from scored s
    left join lateral (
            select c.merchant, c.amount as transaction_amount
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
deduped as (
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        status_length,
        status_filled,
        cumulative_balance,
        status_length_avg_6
    from (
        select *, row_number() over (partition by account_type, merchant order by transaction_amount nulls last, id) as dedup_rank
        from with_latest
    ) t
    where dedup_rank = 1
),
summary as (
    select
        status,
        count(*) filter (where status = 'CLOSED') as closed_rows,
        approx_percentile(status_length_avg_6, 0.5) as status_length_avg_6_p50,
        count_if(account_type = 'CHECKING') as checking_events
    from deduped
    group by status
)
select
    status,
    closed_rows,
    status_length_avg_6_p50,
    checking_events
from summary
order by status
limit 20;
