select id, account_id from transactions where merchant != 'Shell' and amount > 3000 and merchant in ('Starbucks', 'Amazon', 'Lazada')
