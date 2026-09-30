select case when segment in ('Consumer', 'SMB') then 'priority' else 'other' end segment_group, max(credit_limit) - min(credit_limit) credit_limit_range
from customers
where age <= 40 and city in ('Chiang Mai', 'New York')
group by segment_group
order by credit_limit_range desc
