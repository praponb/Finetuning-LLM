select *
from customers
where age != 60 and (segment = 'SMB' or credit_limit < 15000);
