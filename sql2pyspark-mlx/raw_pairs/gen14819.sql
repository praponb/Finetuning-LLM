select
    id,
    store_name,
    region,
    sqft,
    datediff(current_date(), opened_date) days_since_opened_date
from stores
where sqft = 12532 and opened_date <= '2025-06-07';
