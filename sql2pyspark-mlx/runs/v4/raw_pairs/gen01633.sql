with joined as (
    -- Attach customers attributes
    select
        s.*,
        b.signup_date,
        b.email,
        b.city
    from accounts s
    inner join customers b
        on s.customer_id = b.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from joined
    qualify row_number() over (partition by city order by signup_date desc, id) = 1
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, top1.transaction_txn_time
    from latest s
    join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.signup_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
remaining_keys as (
    -- Set intersection on keys
    select account_type
    from with_last
    intersect
    select account_type
    from accounts
    where month(opened_date) in (6, 9, 10)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last s
    inner join remaining_keys k on s.account_type = k.account_type
)
-- Grouped metrics
select
    case when balance >= 82000 then 'LARGE' else 'SMALL' end as balance_tier,
    date_trunc('week', transaction_txn_time) as week_start,
    count(distinct status) as distinct_status,
    avg(balance) filter (where status = 'OPEN') as open_avg_balance,
    percentile_approx(balance, 0.9) as balance_p90,
    sort_array(collect_set(status)) as status_list
from verified
group by cube (case when balance >= 82000 then 'LARGE' else 'SMALL' end, date_trunc('week', transaction_txn_time))
order by distinct_status desc;
