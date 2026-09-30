select rating, country, initcap(trim(country)) country_title
from suppliers
where supplier_name is not null or supplier_name is not null
order by country_title;
