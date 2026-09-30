select merchant, sum(amount) as total_amount, count(*) as cnt, rank() over (order by sum(amount) desc) as rnk from transactions where is_flagged group by merchant order by 1, cnt desc limit 10
