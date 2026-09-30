select location, concat_ws(', ', sort_array(collect_set(location))) as location_values, count(budget) as budget_count
from departments
where not ((location = 'Singapore' and location in ('Tokyo', 'Singapore')) or location = 'Bangkok')
group by location
order by location desc
limit 10
