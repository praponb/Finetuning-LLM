select id, txn_time, is_flagged, case when txn_type = 'CREDIT' then 1 else 0 end as is_credit from transactions where (txn_time < '2024-07-17' and is_flagged) or account_id in (13, 21) order by id
