select year(signup_date) as signup_year, count(*) as row_count
from customers
where (is_active and length(name) > 13) or lower(trim(name)) = 'olivia garcia'
group by year(signup_date)
order by row_count desc, signup_year desc;
