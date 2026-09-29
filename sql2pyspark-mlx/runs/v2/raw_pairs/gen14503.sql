select plan, min_monthly_fee from (select plan, min(monthly_fee) as min_monthly_fee from subscriptions group by plan) as x where min_monthly_fee >= 110;
