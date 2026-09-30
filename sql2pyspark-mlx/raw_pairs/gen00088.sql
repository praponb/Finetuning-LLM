select status, max(amount) - min(amount) as amount_range from orders where amount >= 1590 group by status having count(distinct amount) >= 4
