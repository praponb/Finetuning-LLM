select round(stddev(revenue), 2) as stddev_revenue
from store_sales
where not (revenue <= 1500 and (units is null or revenue >= 2100));
