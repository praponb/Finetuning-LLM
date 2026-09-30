select amount, account_id, merchant, floor(amount) amount_whole, cast(is_flagged as int) is_flagged_flag from transactions order by account_id desc
