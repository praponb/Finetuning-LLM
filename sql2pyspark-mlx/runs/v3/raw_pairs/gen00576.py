from pyspark.sql import functions as F

result = df_store_sales.withColumn("greatest_units_revenue", F.greatest("units", "revenue"))
