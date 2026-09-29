select status, 'account' source
from accounts
where account_type = 'SAVINGS'
union all
select status, 'shipment' source
from shipments
