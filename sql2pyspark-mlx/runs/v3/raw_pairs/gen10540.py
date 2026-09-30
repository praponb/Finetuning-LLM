from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
    .orderBy(F.col("store_id").desc(), "revenue_per_unit")
)
