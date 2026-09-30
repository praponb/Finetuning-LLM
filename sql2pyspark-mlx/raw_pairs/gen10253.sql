with enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(status) ignore nulls over (partition by status order by opened_date, id rows between unbounded preceding and current row) as last_known_status,
        rank() over (partition by date_trunc('YEAR', opened_date), status order by balance nulls first) as balance_rank,
        lead(balance, 2) over (partition by customer_id order by opened_date, id) as next_balance,
        sum(balance) over (partition by status order by opened_date, id rows between unbounded preceding and current row) as cumulative_balance,
        cume_dist() over (partition by customer_id order by balance desc nulls first) as balance_cume_dist
    from accounts
)
select
    id,
    account_type,
    opened_date,
    status,
    last_known_status,
    cumulative_balance,
    case when account_type rlike '^(CREDIT|CHECKING)$' then 'GROUP_A' when account_type rlike '^(SAVINGS|LOAN)$' then 'OTHER_ZONE' else 'CORE' end as account_type_zone
from enriched
order by opened_date desc nulls last, cumulative_balance
