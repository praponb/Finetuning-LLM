select
    id,
    supplier_name,
    city,
    lower(country) as country_lower
from suppliers
