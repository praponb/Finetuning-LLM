select launch_date from products where launch_date is not null union select shipped_date launch_date from shipments where shipped_date is not null order by launch_date desc
