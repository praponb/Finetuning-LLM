select *
from (
    select *, row_number() over (partition by store_id order by quantity desc, id) as rank_in_group
    from inventory
) x
where rank_in_group = 1;
