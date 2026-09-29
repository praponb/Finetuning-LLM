select status, method from payments where amount > 1450 except select status, method from payments where method = 'bank_transfer'
