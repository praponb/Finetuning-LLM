select shipped_date, round(cost * 1.15, 2) as adjusted_cost
from shipments
where delivered_date >= '2025-10-01'
