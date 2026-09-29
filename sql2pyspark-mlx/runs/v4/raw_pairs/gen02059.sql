select
    id,
    category,
    launch_date,
    row_number() over (partition by category order by launch_date desc, id) as launch_date_row
from products
where category in ('Books', 'Grocery');
