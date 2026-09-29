SELECT * FROM suppliers WHERE country != 'DE' AND (city IN ('Berlin', 'London', 'New York') OR country != 'UK');
