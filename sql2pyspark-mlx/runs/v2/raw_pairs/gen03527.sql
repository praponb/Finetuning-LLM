SELECT id, end_date, ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS row_num FROM subscriptions WHERE monthly_fee BETWEEN 190 AND 200;
