select id, status, monthly_fee, dense_rank() over (partition by status order by monthly_fee) as monthly_fee_dense_rank from subscriptions
