select account_id from transactions where amount > 3900 union select account_id from transactions where amount between 2900 and 3500;
