select segment, max(age) - min(age) as age_range
from customers
group by segment
order by segment desc;
