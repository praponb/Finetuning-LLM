select id, opened_date, status from accounts where status is null union select id, opened_date, status from accounts where status != 'FROZEN';
