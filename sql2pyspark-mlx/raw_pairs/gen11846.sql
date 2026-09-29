select sqft, store_name, round(sqft, 1) as sqft_rounded, date_format(opened_date, 'yyyy-MM') as opened_date_label from stores where region = 'East' or length(store_name) < 13
