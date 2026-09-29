select s.id, s.supplier_name, s.country, s.city from suppliers s where s.country not in ('US', 'TH', 'UK') and s.id in (select supplier_id from products where brand in ('Globex', 'Stark'))
