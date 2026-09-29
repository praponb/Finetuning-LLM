select
    id,
    supplier_name,
    rating,
    concat_ws('-', supplier_name, city) as supplier_name_city_key,
    case when rating >= 3.9 then 'High' when rating >= 2.4 then 'Medium' else 'Low' end as rating_level
from suppliers
