select s.delivered_date, s.shipped_date, o.channel, last_day(s.delivered_date) as delivered_date_month_end from shipments s join orders o on s.order_id = o.id where s.cost <= 48;
