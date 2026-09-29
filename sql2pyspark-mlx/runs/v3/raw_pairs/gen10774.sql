SELECT id, sqft, opened_date, DATE_TRUNC('YEAR', opened_date) AS opened_date_period FROM stores ORDER BY sqft DESC, opened_date_period;
