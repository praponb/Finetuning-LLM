select carrier, status, count(distinct cost) as unique_carriers from shipments group by carrier, status order by carrier, unique_carriers desc;
