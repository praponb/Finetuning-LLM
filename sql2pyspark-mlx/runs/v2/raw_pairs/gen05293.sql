select status, 'account' as source
from accounts
union
select status, 'shipment' as source
from shipments;
