select shipped_date
from shipments
union
select opened_date
from accounts
where opened_date > '2024-12-12'
order by shipped_date desc
