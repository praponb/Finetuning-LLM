select id, region, sqft, initcap(trim(region)) as region_title from stores where sqft <> 12925 order by sqft desc, id desc
