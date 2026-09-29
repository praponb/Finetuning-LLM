select quarter(opened_date) as opened_quarter, min(opened_date) as min_opened_date, concat_ws(', ', sort_array(collect_set(region))) as region_values
from stores
group by opened_quarter
