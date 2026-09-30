select city as label, 'supplier' as origin
from suppliers
where city is not null
union
select location as label, 'department' as origin
from departments
where location is not null
order by origin
