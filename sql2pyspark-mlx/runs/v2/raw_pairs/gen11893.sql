select segment, city, trim(segment) as clean_segment
from customers
where is_active = true
