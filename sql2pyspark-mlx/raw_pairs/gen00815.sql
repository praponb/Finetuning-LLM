with filtered as (
    select *, dayofweek(opened_date) as opened_date_dayofweek
    from accounts
    where status not in ('CLOSED', 'FROZEN')
)
select
    date_trunc('year', opened_date) as year_start,
    case when opened_date_dayofweek >= 67 then 'LARGE' else 'SMALL' end as opened_date_dayofweek_tier,
    status,
    sum(opened_date_dayofweek) as total_opened_date_dayofweek,
    percentile_approx(opened_date_dayofweek, array(0.25, 0.5, 0.75)) as opened_date_dayofweek_quartiles,
    count(distinct status) as distinct_status,
    round(avg(balance), 2) as avg_balance,
    size(collect_set(account_type)) as num_account_types,
    grouping_id() as grouping_level
from filtered
group by cube (date_trunc('year', opened_date), case when opened_date_dayofweek >= 67 then 'LARGE' else 'SMALL' end, status)
order by year_start asc
