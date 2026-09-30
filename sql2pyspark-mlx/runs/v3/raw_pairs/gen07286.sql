select count(distinct rating) as unique_countrys from suppliers where city = 'London' and supplier_name like 'L%' order by unique_countrys
