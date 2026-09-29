select status, carrier, round((100 * sum(case when carrier = 'DHL' then 1 else 0 end)) / count(*), 1) as pct_dhl, min(shipped_date) as min_shipped_date from shipments group by 1, 2;
