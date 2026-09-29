select quarter(signup_date) as signup_quarter, count(case when segment = 'SMB' then 1 end) as smb_count
from customers
where is_active
group by signup_quarter
order by smb_count desc, signup_quarter desc
limit 50;
