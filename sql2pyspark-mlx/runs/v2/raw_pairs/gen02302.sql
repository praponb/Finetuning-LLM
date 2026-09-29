select id, account_id, txn_type from transactions where amount between 1500 and 2100 except select id, account_id, txn_type from transactions where merchant = '7-Eleven'
