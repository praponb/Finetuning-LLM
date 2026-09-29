from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.col("revenue") >= 3500, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") > 17)
)
