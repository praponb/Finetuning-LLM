select id, region from stores where sqft > 11010 union select id, region from stores where lower(store_name) like '%own%'
