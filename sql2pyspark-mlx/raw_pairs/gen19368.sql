with with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.balance,
        e.opened_date,
        c.credit_limit,
        c.signup_date
    from accounts e
    join customers c
        on e.customer_id <=> c.id
    where c.is_active
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        c.segment,
        c.credit_limit as customer_credit_limit
    from with_dims a
    join customers c
        on a.customer_id = c.id
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.balance,
        s.credit_limit,
        s.customer_credit_limit,
        top1.txn_time
    from joined s
    left join lateral (
            select c.txn_time
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.signup_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lag(customer_credit_limit, 2, 0) over (order by credit_limit, id) as prev_customer_credit_limit,
        row_number() over (order by customer_credit_limit, id) as seq_no,
        avg(credit_limit) over (order by customer_credit_limit, id rows between 5 preceding and current row) as credit_limit_avg_5
    from with_latest
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        balance,
        credit_limit,
        txn_time,
        date_format(txn_time, 'yyyy-MM-dd') as txn_time_label
    from scored
    where customer_credit_limit > 13200
),
summary as (
    -- Grouped metrics
    select
        date_trunc('week', txn_time) as week_start,
        case when balance >= 78000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
        max(credit_limit) as max_credit_limit,
        approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
        count(distinct id) as distinct_ids,
        stddev_samp(balance) as balance_stddev
    from cleaned
    group by 1, 2
)
select
    week_start,
    balance_tier,
    max_credit_limit,
    balance_stddev
from summary
order by week_start, balance_stddev desc;
