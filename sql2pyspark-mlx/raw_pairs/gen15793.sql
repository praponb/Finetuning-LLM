select merchant, sum(amount) as total_amount, rank() over (order by sum(amount) desc) as amount_rank from transactions where amount > 3800 group by merchant order by merchant, amount_rank
