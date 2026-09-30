select case when location in ('Bangkok', 'New York') then 'group_a' else 'other' end location_group, case when budget >= 3213000 then 'high' when budget >= 958000 then 'mid' else 'low' end budget_bucket, sum(case when location = 'Bangkok' then budget else 0 end) bangkok_budget
from departments
where location in ('Berlin', 'Bangkok')
group by case when location in ('Bangkok', 'New York') then 'group_a' else 'other' end, case when budget >= 3213000 then 'high' when budget >= 958000 then 'mid' else 'low' end
having bangkok_budget > 50;
