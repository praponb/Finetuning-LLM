select
    id,
    country,
    supplier_name,
    city,
    concat(supplier_name, ' ', city) as supplier_name_city,
    upper(trim(supplier_name)) as supplier_name_normalized
from suppliers
where country in ('JP', 'SG') or rating between 2.3 and 4.0
order by id asc;
