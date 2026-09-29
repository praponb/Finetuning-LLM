select segment, sum(age) total_age, rank() over (order by sum(age) desc) as age_rank from customers where is_active group by segment order by segment asc
