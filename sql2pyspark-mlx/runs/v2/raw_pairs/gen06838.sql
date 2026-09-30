select id, opened_date, region
from stores
where opened_date = (select max(opened_date) from stores)
order by region asc
