select cost, cast(cost as int) as cost_int
from shipments
where not ((delivered_date >= '2023-12-26' and cost between 43 and 91) or delivered_date < '2025-03-28')
