select supplier_name from suppliers where supplier_name like 'G%' union all select supplier_name from suppliers where city in ('London', 'Berlin') order by supplier_name
