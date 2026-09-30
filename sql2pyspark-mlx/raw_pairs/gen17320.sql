with cleaned as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        trim(account_type) clean_account_type,
        case when account_type = 'SAVINGS' then 1 else 0 end is_savings
    from accounts
    where customer_id is not null
),
kept as (
    select s.*
    from cleaned s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time <= s.opened_date
        )
),
with_dims as (
    select
        s.id,
        s.customer_id,
        s.is_savings,
        p.segment,
        p.credit_limit,
        p.signup_date
    from kept s
    left join customers p
        on s.customer_id <=> p.id
),
with_latest as (
    select
        s.*,
        top1.txn_time,
        top1.merchant
    from with_dims s
    left join lateral (
            select c.txn_time, c.merchant
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.signup_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    select s.*
    from with_latest s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time <= s.signup_date
        )
),
with_windows as (
    select
        id,
        customer_id,
        segment,
        txn_time,
        merchant,
        min(is_savings) over w_running last4_min_is_savings,
        count(is_savings) over w_running last4_count_is_savings,
        max(is_savings) over w_running last4_max_is_savings,
        avg(credit_limit) over (partition by customer_id order by credit_limit, id rows between 2 preceding and 1 preceding) credit_limit_avg_2_prior
    from eligible
    window w_running as (partition by merchant order by is_savings, id rows between 3 preceding and current row)
),
with_dims_accounts as (
    select
        a.id,
        a.customer_id,
        a.segment,
        a.merchant,
        a.last4_count_is_savings,
        a.credit_limit_avg_2_prior,
        d.city
    from with_windows a
    inner join customers d
        on a.customer_id = d.id
),
wide as (
    select *
    from (
        select
            customer_id,
            merchant,
            credit_limit_avg_2_prior
        from with_dims_accounts
    ) src
    pivot (
        avg(credit_limit_avg_2_prior) as value_sum,
        count(1) as item_count
        for merchant in ('Lazada' as lazada, 'Starbucks' as starbucks, 'Amazon' as amazon)
    )
)
select
    s.customer_id,
    s.lazada_value_sum,
    s.lazada_item_count,
    s.starbucks_item_count,
    s.amazon_value_sum,
    s.amazon_item_count,
    (select sum(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as sum_account_balance
from wide s
order by lazada_value_sum desc nulls first, lazada_item_count desc;
