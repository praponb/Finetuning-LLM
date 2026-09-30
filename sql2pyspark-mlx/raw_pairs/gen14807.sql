select supplier_name, upper(country) as country_upper
from suppliers
where country in ('TH', 'SG', 'UK')
