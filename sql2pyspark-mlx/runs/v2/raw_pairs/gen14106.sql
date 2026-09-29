select name, cast(credit_limit as int) as credit_limit_int, initcap(trim(segment)) as segment_title
from customers
where age >= 35 and credit_limit between 6800 and 11200 and segment in ('Corporate', 'Consumer', 'SMB')
order by credit_limit_int desc
