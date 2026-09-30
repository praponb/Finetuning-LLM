select *, case when level in ('L5', 'L1') then 'Tier 1' else 'Other' end as level_group from employees where level in ('L2', 'L5') or email like '%@gmail.com' order by salary, dept_id, id limit 20;
