select id, customer_id, monthly_fee, round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 3) as monthly_fee_share from subscriptions where status in ('EXPIRED', 'ACTIVE')
