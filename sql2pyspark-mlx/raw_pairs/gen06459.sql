SELECT id, location FROM departments EXCEPT ALL SELECT id, location FROM departments WHERE location NOT IN ('London', 'Berlin', 'New York');
