select s.city, s.country, s.rating, p.stock_qty from suppliers s left outer join products p on p.supplier_id = s.id where p.brand <> 'Umbrella' order by s.rating desc;
