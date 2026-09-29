select supplier_name, cast(rating as int) as rating_int
from suppliers
where upper(trim(supplier_name)) = 'EUROGOODS GMBH' and country in ('JP', 'TH')
order by rating_int desc
