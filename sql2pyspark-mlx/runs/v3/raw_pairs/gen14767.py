from pyspark.sql import functions as F

result = df_departments.withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
