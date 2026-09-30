select city, round((100 * sum(case when segment = 'SMB' then 1 else 0 end)) / count(*), 1) as pct_smb, sum(case when country = 'TH' then 1 else 0 end) as th_count
from customers
where segment in ('SMB', 'Corporate') and city in ('Berlin', 'New York', 'Bangkok') and signup_date <= '2023-05-01'
group by city
