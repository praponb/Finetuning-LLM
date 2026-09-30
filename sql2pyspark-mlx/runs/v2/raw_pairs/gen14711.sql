SELECT account_type, status FROM accounts WHERE balance BETWEEN 35000 AND 52000 INTERSECT SELECT account_type, status FROM accounts WHERE balance > 46000;
