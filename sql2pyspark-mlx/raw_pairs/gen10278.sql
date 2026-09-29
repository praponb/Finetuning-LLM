select id, sqft
from stores
where sqft < (select min(sqft) from stores where sqft between 4790 and 8439);
