select carrier, quarter(shipped_date) as shipped_quarter, max(cost) - min(cost) as cost_range
from shipments
where carrier in ('Flash', 'Kerry', 'DHL')
group by carrier, shipped_quarter
having cost_range >= 27
order by shipped_quarter desc;
