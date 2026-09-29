select supplier_name, case when rating >= 4.4 then 'High' when rating >= 1.9 then 'Medium' else 'Low' end as rating_tier, length(supplier_name) as supplier_name_length
from suppliers
where country = 'JP' and city = 'Singapore'
