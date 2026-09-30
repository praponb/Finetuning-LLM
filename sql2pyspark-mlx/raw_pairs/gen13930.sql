SELECT id, region FROM stores EXCEPT SELECT id, region FROM stores WHERE city = 'Bangkok';
