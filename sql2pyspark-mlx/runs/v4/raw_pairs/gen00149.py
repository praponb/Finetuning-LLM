from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.when(F.col("ss.units") >= 45, "high").when(F.col("ss.units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 90)
    .orderBy(F.col("units_bucket").desc(), "avg_quantity")
)
