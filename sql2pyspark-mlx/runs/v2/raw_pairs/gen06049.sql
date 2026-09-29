select amount, status, method from payments where amount <= 1110 and method not in ('card', 'promptpay');
