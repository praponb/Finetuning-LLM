with valid_stores as (
    select *
    from stores
    where year(opened_date) = 2025
),
summary as (
    select
        date_trunc('MONTH', opened_date) as opened_month,
        region,
        min(opened_date) as first_opened_date,
        count(*) as cnt
    from valid_stores
    group by date_trunc('MONTH', opened_date), region
)
select *
from summary;
