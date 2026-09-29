select account_id from transactions where amount <= 1800 intersect select account_id from transactions where account_id in (20, 23, 36, 39);
