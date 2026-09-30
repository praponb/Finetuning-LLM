with valid as (
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        upper(account_type) as account_type_upper,
        date_format(opened_date, 'yyyy-MM') as opened_date_label,
        coalesce(status, 'DIRECT') as resolved_status
    from accounts
    where status is not null
),
scored as (
    select
        *,
        sum(balance) over w_recent as cumulative_balance,
        max(balance) over (partition by account_type, status) as account_type_max_balance,
        avg(balance) over (partition by account_type order by opened_date, id rows between 3 preceding and current row) as balance_avg_3
    from valid
    window w_recent as (order by opened_date, id rows between unbounded preceding and current row)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by status order by opened_date desc, id) as rn
        from scored
    ) x
    where rn = 1
)
select
    date_trunc('week', opened_date) as week_start,
    case when account_type_max_balance >= 59925 then 'LARGE' else 'SMALL' end as account_type_max_balance_tier,
    case when cumulative_balance >= 391581 then 'LARGE' else 'SMALL' end as cumulative_balance_tier,
    count(*) as event_count,
    avg(balance_avg_3) as avg_balance_avg_3,
    count(case when account_type = 'CREDIT' then 1 end) as credit_count,
    percentile_approx(account_type_max_balance, 0.95) as account_type_max_balance_p95,
    size(collect_set(account_type)) as num_account_types,
    percent_rank() over (partition by date_trunc('week', opened_date) order by percentile_approx(account_type_max_balance, 0.95) desc) as account_type_max_balance_p95_percent_rank
from ranked
group by date_trunc('week', opened_date), case when account_type_max_balance >= 59925 then 'LARGE' else 'SMALL' end, case when cumulative_balance >= 391581 then 'LARGE' else 'SMALL' end
order by week_start desc;
