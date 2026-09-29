from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-04-01")
    .withColumn("revenue_per_unit", (F.col("revenue") - F.col("units")))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)
