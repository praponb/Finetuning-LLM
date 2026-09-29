with base as (
    select *, case when status in ('REFUNDED', 'FAILED') then 'Tier 1' else 'Other' end as status_group
    from payments
    where paid_at < '2025-04-28'
),
Summary as (
    select method, max(amount) as max_amount, round(avg(amount), 2) as avg_amount
    from base
    group by method
)
select *
from Summary
