from pyspark.sql import functions as F

result = df_employees.filter(F.col("email").like("%@mail.co.th") | (F.col("dept_id") == 26))
