select 'payment' as kind, method from payments union select 'account' as kind, account_type from accounts where account_type = 'CHECKING';
