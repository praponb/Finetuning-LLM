SELECT status FROM accounts WHERE balance BETWEEN 42000 AND 77000 INTERSECT SELECT status FROM payments WHERE method != 'cash';
