WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) row_num FROM subscriptions) SELECT * FROM numbered WHERE row_num <= 5
