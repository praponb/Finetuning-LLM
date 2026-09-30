with recent_payments as (
    select *
    from payments
    where amount > 550 and method in ('bank_transfer', 'cash')
),
summary as (
    select case when method in ('bank_transfer', 'cash') then 'tier_1' else 'other' end as method_group, status, count(*) as cnt
    from recent_payments
    group by case when method in ('bank_transfer', 'cash') then 'tier_1' else 'other' end, status
)
select method_group, status, cnt
from summary
where cnt > 9
order by status;
