select id, merchant, txn_type from transactions where merchant in ('Starbucks', 'Shell', 'Lazada') order by id desc;
