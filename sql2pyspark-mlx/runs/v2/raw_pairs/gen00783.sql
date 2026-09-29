select status, unique_statuss from (select status, count(distinct monthly_fee) as unique_statuss from subscriptions group by status) as sub where unique_statuss >= 4
