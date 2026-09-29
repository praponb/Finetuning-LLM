select brand, sum(case when category = 'Sports' then 1 else 0 end) as sports_count, count(distinct cost) as unique_brands, count(distinct stock_qty) as unique_suppliers from products group by brand;
