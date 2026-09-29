SELECT id FROM suppliers EXCEPT DISTINCT SELECT id FROM suppliers WHERE rating > 4.0;
