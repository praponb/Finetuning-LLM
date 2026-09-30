select
    price,
    cost,
    add_months(launch_date, 6) as launch_date_plus_months,
    cost - stock_qty as cost_minus_stock_qty
from products;
