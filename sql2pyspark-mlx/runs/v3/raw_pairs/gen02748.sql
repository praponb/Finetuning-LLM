select shipped_date, 'shipment' as source
from shipments
where year(shipped_date) = 2025
union all
select opened_date, 'account' as source
from accounts;
