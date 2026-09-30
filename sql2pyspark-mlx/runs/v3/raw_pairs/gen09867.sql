WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS rnk FROM subscriptions) SELECT * FROM numbered WHERE rnk = 1 ORDER BY rnk;
