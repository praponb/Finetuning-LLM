select txn_time, is_flagged from transactions where amount < 1600 order by txn_time;
