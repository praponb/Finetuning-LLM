select account_id, is_flagged, length(merchant) as merchant_length from transactions order by merchant_length desc
