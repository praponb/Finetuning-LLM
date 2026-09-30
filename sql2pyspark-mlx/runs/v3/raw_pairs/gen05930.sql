select * from (select quarter(last_restock) as last_restock_quarter, min(quantity) as min_quantity from inventory group by quarter(last_restock)) as s where min_quantity >= 130 order by 1 desc
