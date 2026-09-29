select
    carrier,
    count(case when carrier = 'DHL' then 1 end) as dhl_count,
    sum(case when status = 'RETURNED' then cost else 0 end) as returned_cost,
    count(*) as cnt
from shipments
group by carrier
