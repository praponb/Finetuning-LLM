with recent_shipments as (
    select *
    from shipments
    where cost <= 68
),
status_group_summary as (
    select case when status in ('DELIVERED', 'RETURNED') then 'core' else 'other' end as status_group, count(case when carrier = 'FedEx' then 1 end) as fedex_count
    from recent_shipments
    group by case when status in ('DELIVERED', 'RETURNED') then 'core' else 'other' end
)
select status_group, fedex_count
from status_group_summary;
