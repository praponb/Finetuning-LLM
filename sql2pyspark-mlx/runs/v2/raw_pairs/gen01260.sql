select status, date_format(start_date, 'yyyy-MM') start_month, min(start_date) min_start_date from subscriptions group by status, date_format(start_date, 'yyyy-MM') order by status
