SELECT id FROM sessions EXCEPT SELECT id FROM sessions WHERE device != 'mobile';
