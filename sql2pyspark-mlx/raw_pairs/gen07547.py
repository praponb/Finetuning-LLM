from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.product_id").isin([3, 5, 10]))
    .groupBy(
        F.when(F.col("s.city").isin(["London", "Chiang Mai"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
    .orderBy("last_sale_date")
)
