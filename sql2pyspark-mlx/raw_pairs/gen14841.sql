select carrier, count(case when carrier = 'Flash' then 1 end) as flash_count
from shipments
group by 1
having flash_count > 2;
