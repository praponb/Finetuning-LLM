select
    opened_date,
    sqft,
    trim(region) as clean_region,
    datediff(current_date(), opened_date) / 7 as weeks_since_opened_date
from stores
where length(store_name) < 6
