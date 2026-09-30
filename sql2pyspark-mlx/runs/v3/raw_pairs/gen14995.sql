select name from customers where not is_active union select name from customers where is_active;
