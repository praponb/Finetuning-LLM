select a.id, a.opened_date from accounts a where a.id in (select account_id from transactions where merchant in ('Lazada', 'Starbucks', 'Amazon'));
