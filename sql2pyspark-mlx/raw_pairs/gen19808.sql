with cleaned as (
    select
        id,
        name,
        signup_date,
        age,
        month(signup_date) as signup_date_month
    from customers
    where segment = 'SMB'
),
remaining_keys as (
    select id
    from cleaned
    intersect all
    select id
    from customers
    where country in ('UK', 'US', 'JP')
),
reconciled as (
    select s.*
    from cleaned s
    join remaining_keys k on s.id = k.id
),
kept as (
    select s.*
    from reconciled s
    where not exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.device in ('desktop', 'mobile')
        )
),
enriched as (
    select
        name,
        signup_date,
        dense_rank() over (order by signup_date_month desc nulls first) as signup_date_month_dense_rank,
        rank() over (order by age desc) as age_rank
    from kept
),
filtered as (
    select *, age_rank - signup_date_month_dense_rank as age_rank_minus_signup_date_month_dense_rank
    from enriched
    where length(name) > 8
),
with_windows as (
    select
        name,
        signup_date,
        signup_date_month_dense_rank,
        age_rank_minus_signup_date_month_dense_rank,
        dense_rank() over (order by signup_date_month_dense_rank asc) as signup_date_month_dense_rank_dense_rank,
        cume_dist() over (order by age_rank desc) as age_rank_cume_dist
    from filtered
),
scored as (
    select *, dense_rank() over (order by signup_date_month_dense_rank_dense_rank desc nulls last) as signup_date_month_dense_rank_dense_rank_dense_rank
    from with_windows
)
select
    date_trunc('month', signup_date) as activity_month,
    sum(signup_date_month_dense_rank_dense_rank) as total_signup_date_month_dense_rank_dense_rank,
    round(avg(signup_date_month_dense_rank_dense_rank_dense_rank), 2) as avg_signup_date_month_dense_rank_dense_rank_dense_rank
from scored
group by activity_month
order by activity_month desc nulls last
