select carrier, count(distinct cost) as unique_orders
from shipments
where status != 'RETURNED' and year(delivered_date) >= 2025
group by carrier
having unique_orders > 1
order by unique_orders desc, carrier
limit 10;
