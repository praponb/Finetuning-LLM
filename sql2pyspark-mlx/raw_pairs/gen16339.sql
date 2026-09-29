select status
from shipments
where delivered_date <= '2024-08-01'
union
select status
from shipments
where month(shipped_date) in (3, 4, 9)
order by status desc;
