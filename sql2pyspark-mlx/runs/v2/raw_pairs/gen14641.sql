select supplier_id, category from products where brand = 'Acme' union select supplier_id, category from products where stock_qty >= 325
