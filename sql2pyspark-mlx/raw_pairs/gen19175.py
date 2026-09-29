from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue") >= 3500)
    .groupBy(
        F.when(F.col("ss.units") >= 44, "high").when(F.col("ss.units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        F.month(F.col("ss.sale_date")).alias("sale_month"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("max_quantity") > 160)
    .orderBy(F.col("total_quantity").desc(), F.col("max_quantity").desc())
)
