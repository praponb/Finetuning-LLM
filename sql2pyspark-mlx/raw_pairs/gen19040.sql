SELECT id, store_name, opened_date, region, DATE_FORMAT(opened_date, 'yyyy-MM') opened_date_label FROM stores WHERE opened_date <= '2025-10-01';
