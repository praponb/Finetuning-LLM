select carrier, count(case when status = 'DELIVERED' then 1 end) as delivered_count, max(shipped_date) as latest_shipped_date
from shipments
group by carrier;
