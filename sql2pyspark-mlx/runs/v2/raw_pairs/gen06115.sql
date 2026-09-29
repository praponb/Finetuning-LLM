SELECT id, region, DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label FROM stores WHERE city <> 'New York';
