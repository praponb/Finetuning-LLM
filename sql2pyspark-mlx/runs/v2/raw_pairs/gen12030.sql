select *, upper(country) as country_upper
from suppliers
where supplier_name is not null and country in ('UK', 'JP')
order by rating desc, id
limit 5;
