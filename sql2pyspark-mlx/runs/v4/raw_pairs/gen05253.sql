select status, carrier, round(avg(cost), 2) avg_cost
from shipments
where delivered_date > '2024-08-01'
group by 1, 2
