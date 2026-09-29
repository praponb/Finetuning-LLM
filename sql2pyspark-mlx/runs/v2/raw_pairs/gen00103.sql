select browser, customer_id from sessions where device in ('mobile', 'desktop', 'tablet') union all select browser, customer_id from sessions where customer_id = 32 order by customer_id
