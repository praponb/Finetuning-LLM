select
    id,
    carrier,
    round(cost * 1.1, 2) as cost_scaled,
    upper(status) as status_upper
from shipments
order by cost_scaled, id
limit 5;
