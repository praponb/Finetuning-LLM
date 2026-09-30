select delivered_date from shipments intersect select opened_date from stores order by delivered_date desc
