select *
from stores
where month(opened_date) in (2, 7, 9);
