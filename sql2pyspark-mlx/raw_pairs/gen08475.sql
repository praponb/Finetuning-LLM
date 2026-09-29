select *, row_number() over (partition by location order by budget desc, id) as rank_in_group
from departments
qualify rank_in_group = 1;
