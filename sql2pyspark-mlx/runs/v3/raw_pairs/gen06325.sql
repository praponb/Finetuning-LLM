SELECT id, opened_date FROM accounts WHERE opened_date = (SELECT MAX(opened_date) FROM accounts WHERE balance < 52000);
