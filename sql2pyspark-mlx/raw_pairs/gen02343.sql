select id, channel
from promotions
except
select id, channel
from promotions
where year(ends_on) >= 2023
