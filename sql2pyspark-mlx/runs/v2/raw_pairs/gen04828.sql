select tags, stock_qty, last_day(launch_date) launch_date_month_end
from products
order by launch_date_month_end asc
