select a.id, a.status from accounts a where a.id in (select account_id from transactions where merchant <> 'Starbucks');
