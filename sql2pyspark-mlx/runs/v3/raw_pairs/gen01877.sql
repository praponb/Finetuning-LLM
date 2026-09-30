SELECT *, WEEKOFYEAR(end_date) AS end_date_weekofyear, LAST_DAY(start_date) AS start_date_month_end FROM subscriptions
