select id, account_id, length(merchant) merchant_length, cast(is_flagged as int) is_flagged_flag from transactions where merchant in ('Grab', 'Amazon') or amount between 3600 and 4000;
