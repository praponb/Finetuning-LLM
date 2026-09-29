from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3200, 3800) & (F.col("revenue") >= 3900) & (F.col("units") < 34))
    .withColumn("greatest_units_revenue", F.greatest("units", "revenue"))
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
    .orderBy(F.col("product_id").desc())
)
