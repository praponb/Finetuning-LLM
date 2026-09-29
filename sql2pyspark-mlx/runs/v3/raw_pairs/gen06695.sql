select txn_type, is_flagged, merchant from transactions where merchant in ('Shell', 'Lazada') and amount < 3400 and account_id in (15, 16, 40)
