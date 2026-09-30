SELECT city, region FROM stores INTERSECT SELECT city, region FROM stores WHERE city IN ('Singapore', 'New York')
