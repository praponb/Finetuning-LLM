select status, 'account' as source
from accounts
where status = 'OPEN'
union
select status, 'shipment' as source
from shipments;
