select
    id,
    status,
    carrier,
    case when status in ('RETURNED', 'IN_TRANSIT') then 'Tier 1' else 'Other' end as status_group,
    initcap(trim(status)) as status_title
from shipments
where order_id in (8, 14, 16, 37)
order by status desc;
