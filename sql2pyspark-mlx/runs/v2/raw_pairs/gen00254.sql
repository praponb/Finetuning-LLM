select country
from suppliers
union
select country
from sessions
order by country;
