select city, min(opened_date) first_opened_date
from stores
group by city
order by first_opened_date, 1 desc
