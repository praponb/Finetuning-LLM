from pyspark.sql import functions as F

result = df_store_sales.withColumn("greatest_revenue_units", F.greatest("revenue", "units"))
