from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]))
    .groupBy(
        F.when(F.col("units") >= 31, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.sum("units").alias("total_units"),
    )
    .orderBy("last_sale_date")
)
