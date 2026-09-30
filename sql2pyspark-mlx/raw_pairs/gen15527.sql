select *, cast(is_flagged as int) as is_flagged_flag, datediff(current_date(), txn_time) / 365 as years_since_txn_time from transactions where account_id in (14, 29) order by 8, merchant;
