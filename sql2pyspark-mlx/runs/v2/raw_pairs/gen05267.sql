select *, round(units, 0) units_rounded
from store_sales
where year(sale_date) = 2023 and month(sale_date) in (4, 6, 10) and units = 29
