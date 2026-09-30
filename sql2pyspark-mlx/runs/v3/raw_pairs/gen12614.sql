SELECT id, account_type, ADD_MONTHS(opened_date, 1) opened_date_plus_months FROM accounts WHERE account_type IN ('LOAN', 'SAVINGS', 'CREDIT') AND account_type = 'CHECKING' ORDER BY account_type;
