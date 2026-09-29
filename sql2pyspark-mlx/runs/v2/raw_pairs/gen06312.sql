select month(opened_date) as opened_month, min(opened_date) as first_opened_date
from stores
group by 1
order by opened_month desc;
