select country, sort_array(collect_set(segment)) segment_list
from customers
group by country
order by country desc;
