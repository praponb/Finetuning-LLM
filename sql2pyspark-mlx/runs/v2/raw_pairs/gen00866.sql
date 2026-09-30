select sort_array(collect_set(country)) country_list
from customers
where age is not null
