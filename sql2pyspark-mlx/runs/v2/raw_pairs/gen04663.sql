select case when age >= 61 then 'high' when age >= 29 then 'mid' else 'low' end as age_bucket, count(case when country = 'SG' then 1 end) as sg_count, count(*) as customer_count
from customers
where upper(trim(email)) = 'JACK@STARTUP.IO'
group by case when age >= 61 then 'high' when age >= 29 then 'mid' else 'low' end
having customer_count >= 5
order by age_bucket
