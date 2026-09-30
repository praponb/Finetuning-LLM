select *, case when txn_type = 'REFUND' then 1 else 0 end is_refund, trim(merchant) clean_merchant from transactions where merchant in ('Amazon', 'Starbucks')
