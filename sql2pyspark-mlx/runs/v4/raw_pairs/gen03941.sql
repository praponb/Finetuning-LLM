SELECT account_type, id FROM accounts INTERSECT SELECT account_type, id FROM accounts WHERE balance > 43000;
