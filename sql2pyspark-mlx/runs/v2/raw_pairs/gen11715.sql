select case when d.budget >= 4444000 then 'high' when d.budget >= 837000 then 'mid' else 'low' end as budget_bucket, count(case when e.level = 'L1' then 1 end) as l1_count
from employees e
left join departments d on e.dept_id = d.id
where d.budget between 1589000 and 3031000
group by case when d.budget >= 4444000 then 'high' when d.budget >= 837000 then 'mid' else 'low' end
having count(case when e.level = 'L1' then 1 end) >= 2
order by l1_count desc;
