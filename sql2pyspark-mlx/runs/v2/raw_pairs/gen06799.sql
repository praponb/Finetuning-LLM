select
    id,
    status,
    cost,
    row_number() over (partition by status order by cost desc, id) as rn
from shipments;
