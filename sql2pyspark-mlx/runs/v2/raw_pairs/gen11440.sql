select balance, weekofyear(opened_date) as opened_date_weekofyear from accounts where balance >= 21000 or balance <= 72000;
