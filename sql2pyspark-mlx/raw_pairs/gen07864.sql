select id, launch_date, category, cost, row_number() over (partition by year(launch_date), category order by cost desc, id) as cost_row from products where category in ('Books', 'Grocery');
