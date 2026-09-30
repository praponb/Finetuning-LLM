select * from (select city, segment, credit_limit from customers) pivot (count(credit_limit) for segment in ('Consumer', 'SMB')) order by city
