with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        account_type,
        status,
        datediff(current_date(), opened_date) / 365 as years_since_opened_date
    from accounts
    where opened_date > date '2023-03-12'
      and balance > 45000
)
-- Multi-level aggregation
select
    customer_id,
    count(distinct status) as distinct_status,
    percentile_approx(years_since_opened_date, 0.95) as years_since_opened_date_p95,
    approx_percentile(years_since_opened_date, 0.5) as years_since_opened_date_p50
from cleaned
group by customer_id
order by years_since_opened_date_p95 nulls last
