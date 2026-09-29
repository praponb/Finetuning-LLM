select id, segment, credit_limit, round(credit_limit / sum(credit_limit) over (partition by segment), 3) as credit_limit_share from customers order by segment;
