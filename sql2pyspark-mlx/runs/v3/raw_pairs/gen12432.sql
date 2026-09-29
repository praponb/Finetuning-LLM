select is_flagged, merchant, count(*) cnt, count(amount) amount_count from transactions group by is_flagged, merchant
