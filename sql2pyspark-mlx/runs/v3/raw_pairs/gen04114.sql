select status, flash_count from (select status, sum(case when carrier = 'Flash' then 1 else 0 end) as flash_count from shipments where carrier != 'UPS' group by status) as agg where flash_count < 62;
