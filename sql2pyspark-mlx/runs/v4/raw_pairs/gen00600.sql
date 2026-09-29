select id, location from departments where location = 'New York' union select id, location from departments where budget <= 1508000 order by id;
