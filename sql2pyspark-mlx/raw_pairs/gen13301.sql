select id, monthly_fee, initcap(trim(plan)) as plan_title from subscriptions order by monthly_fee, plan_title;
