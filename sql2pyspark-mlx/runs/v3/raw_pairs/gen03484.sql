select segment, name, coalesce(city, 'N/A') city_filled
from customers
where signup_date < '2025-03-01' and length(email) >= 12
order by city_filled, segment
