select
    id,
    budget,
    coalesce(location, 'unassigned') as location_filled,
    upper(location) as location_upper
from departments
where budget < 1349000 and budget > 1981000;
