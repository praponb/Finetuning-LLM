select city, date_trunc('month', opened_date) as activity_month, count(*) as row_count, bit_or(sqft) as sqft_bit_or from stores group by 1, 2 order by sqft_bit_or
