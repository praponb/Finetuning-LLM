select month(opened_date) as opened_month, round(avg(sqft), 2) as avg_sqft
from stores
where (length(store_name) >= 10 and sqft is not null) or region is not null
group by 1;
