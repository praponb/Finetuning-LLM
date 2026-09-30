select id, merchant, count(*) over (partition by merchant) as merchant_count from transactions
