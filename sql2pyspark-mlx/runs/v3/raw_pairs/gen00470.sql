SELECT id, region, DATE_TRUNC('month', opened_date) opened_date_period
FROM stores;
