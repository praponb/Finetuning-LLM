select
    id,
    city,
    opened_date,
    sqft,
    date_sub(opened_date, 77) as opened_date_minus_77d,
    datediff(current_date(), opened_date) as days_since_opened_date
from stores
where (city = 'Berlin' and sqft < 6299) or city in ('Tokyo', 'Berlin')
order by city desc
