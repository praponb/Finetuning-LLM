SELECT DATE_TRUNC('MONTH', opened_date) opened_month, MAX(opened_date) last_opened_date FROM stores WHERE region != 'North' GROUP BY DATE_TRUNC('MONTH', opened_date) ORDER BY 1 ASC
