select count(distinct monthly_fee) unique_plans, sum(case when plan = 'enterprise' then monthly_fee else 0 end) enterprise_monthly_fee from subscriptions order by enterprise_monthly_fee;
