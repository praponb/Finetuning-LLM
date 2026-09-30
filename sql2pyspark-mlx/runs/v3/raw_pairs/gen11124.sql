select id, account_id, is_flagged, ceil(amount) amount_whole from transactions where is_flagged = true;
