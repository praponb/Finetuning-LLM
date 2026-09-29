select status, count(*) as cnt from shipments group by status having count(*) < 1 order by cnt;
