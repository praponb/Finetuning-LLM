select *, coalesce(carrier, 'N/A') as carrier_filled from shipments where cost <= 30 order by shipped_date;
