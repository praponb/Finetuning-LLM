select
    id,
    sqft,
    opened_date,
    datediff(current_date(), opened_date) / 30 as months_since_opened_date,
    round(sqft, 2) as sqft_rounded
from stores
where region <> 'North';
