select
    id,
    tags,
    brand,
    case when brand in ('Acme', 'Umbrella') then 'Group A' else 'Other' end as brand_group
from products
where category in ('Books', 'Sports')
