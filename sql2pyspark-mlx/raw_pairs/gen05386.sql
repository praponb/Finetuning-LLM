select
    id,
    supplier_name,
    case when city in ('Berlin', 'Tokyo') then 'Priority' else 'Other' end as city_group,
    substring(supplier_name, 1, 2) as supplier_name_prefix
from suppliers
where city is not null and supplier_name is not null;
