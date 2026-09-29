select id, city, add_months(opened_date, 12) as opened_date_plus_months
from stores
order by id
limit 5;
