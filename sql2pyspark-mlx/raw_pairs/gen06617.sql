select
    s.supplier_name,
    p.category,
    p.product_name,
    concat_ws(',', s.country, s.city) as country_city_key
from suppliers s
left join products p on s.id = p.supplier_id;
