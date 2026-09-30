select status, min(delivered_date) first_delivered_date from shipments group by status order by first_delivered_date desc;
