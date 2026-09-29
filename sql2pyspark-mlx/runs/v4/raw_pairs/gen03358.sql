SELECT region FROM stores WHERE region != 'South' INTERSECT SELECT region FROM stores WHERE region = 'West'
