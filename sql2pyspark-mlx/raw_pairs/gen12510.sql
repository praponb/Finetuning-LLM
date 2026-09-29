SELECT status FROM payments WHERE amount >= 1570 INTERSECT SELECT country FROM customers WHERE segment = 'Corporate' ORDER BY status;
