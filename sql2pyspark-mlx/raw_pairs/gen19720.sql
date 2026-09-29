select region
from stores
where opened_date < '2023-03-01'
except
select browser
from sessions;
