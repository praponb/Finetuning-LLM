select *, round(balance, 2) as balance_rounded from accounts where status != 'OPEN' and balance >= 70000;
