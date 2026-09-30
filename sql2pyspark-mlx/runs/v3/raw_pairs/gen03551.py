from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(
        F.year(F.col("s.opened_date")).alias("opened_year"),
        F.when(F.col("s.sqft") >= 17472, "high").when(F.col("s.sqft") >= 4439, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") > 1)
    .orderBy("sqft_bucket", F.col("num_inventory").desc(), "opened_year")
    .limit(10)
)
