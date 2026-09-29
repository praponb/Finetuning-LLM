from pyspark.sql import functions as F

result = df_employees.select("id", "manager_id", "level")
