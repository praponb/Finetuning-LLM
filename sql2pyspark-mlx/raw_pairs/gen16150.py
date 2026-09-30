from pyspark.sql import functions as F

result = df_employees.filter(F.col("level").isin(["L2", "L4", "L3"]))
