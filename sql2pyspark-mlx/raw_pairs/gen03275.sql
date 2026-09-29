select id, status, count(*) over (partition by status) as status_count from shipments
