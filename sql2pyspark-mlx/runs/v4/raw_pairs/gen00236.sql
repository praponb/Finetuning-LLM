with joined as (
    select
        a.*,
        d.account_type,
        d.opened_date
    from transactions a
    left join accounts d
        on a.account_id = d.id
)
select
    account_type,
    date_trunc('quarter', txn_time) as quarter_start,
    date_trunc('year', opened_date) as year_start,
    count(*) filter (where txn_type = 'CREDIT') as credit_rows,
    percentile_approx(amount, 0.5) as amount_p50,
    min(txn_time) as started_at,
    sort_array(collect_set(txn_type)) as txn_type_list
from joined
group by account_type, date_trunc('quarter', txn_time), date_trunc('year', opened_date)
order by quarter_start desc nulls first
