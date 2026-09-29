select carrier, status, shipped_date, trim(status) as clean_status from shipments where status = 'IN_TRANSIT' order by carrier;
