with scored as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        count(balance) over (partition by status, customer_id order by opened_date, id rows between 2 preceding and current row) as last3_count_balance,
        min(balance) over (partition by status, customer_id order by opened_date, id rows between 2 preceding and current row) as last3_min_balance,
        sum(balance) over (partition by status, customer_id order by opened_date, id rows between 2 preceding and current row) as last3_sum_balance,
        max(balance) over (partition by status, customer_id order by opened_date, id rows between 2 preceding and current row) as last3_max_balance
    from accounts
)
select
    case when last3_min_balance >= 86000 then 'PREMIUM' else 'STANDARD' end as last3_min_balance_tier,
    account_type,
    date_trunc('year', opened_date) as year_start,
    sort_array(collect_set(status)) as status_list,
    count(*) filter (where account_type = 'LOAN') as loan_rows,
    grouping_id() as grouping_level
from scored
group by cube (case when last3_min_balance >= 86000 then 'PREMIUM' else 'STANDARD' end, account_type, date_trunc('year', opened_date))
order by last3_min_balance_tier;
