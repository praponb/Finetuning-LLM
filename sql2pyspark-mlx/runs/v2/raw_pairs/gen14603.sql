select supplier_name, concat(supplier_name, ', ', city) supplier_name_city from suppliers where rating > 4.0 and country in ('US', 'TH') and city = 'Bangkok';
