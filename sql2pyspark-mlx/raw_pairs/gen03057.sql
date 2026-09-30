SELECT *, DATE_TRUNC('year', opened_date) AS opened_date_period, ABS(balance) AS abs_balance FROM accounts WHERE balance > 41000 ORDER BY opened_date_period DESC
