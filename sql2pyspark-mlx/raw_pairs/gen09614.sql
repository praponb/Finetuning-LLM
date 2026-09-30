select *, greatest(revenue, units) as greatest_revenue_units
from store_sales
