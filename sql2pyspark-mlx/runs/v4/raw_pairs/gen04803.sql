select id, opened_date
from accounts
where opened_date > '2023-04-01'
order by opened_date, id
limit 10;
