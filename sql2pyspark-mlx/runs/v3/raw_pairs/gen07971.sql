select carrier, max(shipped_date) as last_shipped_date, sum(case when carrier = 'DHL' then 1 else 0 end) as dhl_count
from shipments
group by carrier
having sum(case when carrier = 'DHL' then 1 else 0 end) > 46
