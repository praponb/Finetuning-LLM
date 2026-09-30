with metrics as (
    select
        *,
        avg(amount) over (order by txn_time, id rows between 5 preceding and 1 preceding) as amount_avg_5_prior,
        round(amount / sum(amount) over (partition by date_trunc('MONTH', txn_time), account_id), 4) as amount_share,
        sum(amount) over (partition by txn_type) as txn_type_sum_amount,
        sum(amount) over (partition by account_id, txn_type) as account_id_sum_amount,
        percent_rank() over (partition by txn_type order by amount desc) as amount_percent_rank
    from transactions
),
enriched as (
    select
        src.id,
        src.account_id,
        src.amount,
        src.txn_time,
        src.txn_type_sum_amount,
        src.account_id_sum_amount,
        a.account_type
    from metrics src
    join accounts a
        on src.account_id = a.id
    where a.customer_id in (2, 3, 34)
),
metrics_transactions as (
    select
        id,
        amount,
        txn_time,
        account_id_sum_amount,
        account_type,
        round(account_id_sum_amount / sum(account_id_sum_amount) over (partition by account_id), 4) as account_id_sum_amount_share,
        ntile(5) over (partition by account_type order by txn_type_sum_amount desc, id) as txn_type_sum_amount_bucket,
        cume_dist() over (partition by account_type order by txn_type_sum_amount desc nulls last) as txn_type_sum_amount_cume_dist,
        sum(account_id_sum_amount) over (partition by account_type order by txn_time, id rows between unbounded preceding and current row) as cumulative_account_id_sum_amount,
        lag(account_id_sum_amount, 1) over (partition by month(txn_time), account_type order by amount, id) as prev_account_id_sum_amount
    from enriched
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by account_type order by txn_time desc, id) as rn
        from metrics_transactions
    ) ranked
    where rn = 1
)
select
    id,
    amount,
    txn_time,
    account_type,
    account_id_sum_amount_share,
    prev_account_id_sum_amount,
    rn,
    case when account_type rlike '^(LOAN|CREDIT)$' then 'OTHER_ZONE' when account_type rlike '^(SAVINGS|CHECKING)$' then 'PRIORITY' else 'CORE' end as account_type_zone
from deduped
order by rn desc nulls last
