with with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.amount,
        top1.merchant
    from accounts s
    left join lateral (
            select c.amount, c.merchant
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', opened_date) as quarter_start,
    customer_id,
    round(avg(balance), 2) as avg_balance,
    percentile_approx(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    count(case when merchant = 'Amazon' then 1 end) as amazon_count,
    size(collect_set(status)) as num_status
from with_last
group by grouping sets ((date_trunc('quarter', opened_date), customer_id), (date_trunc('quarter', opened_date)));
