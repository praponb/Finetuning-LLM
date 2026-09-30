from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select("id", "customer_id", F.col("li_item.product_id"), F.col("li_item.promo"))
)

result = (
    exploded
    .groupBy("promo", "product_id")
    .agg(
        F.avg("product_id").alias("avg_product_id"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.avg("product_id").alias("avg_product_id_2"),
        F.avg(F.when(F.col("promo") == "WELCOME5", F.col("product_id"))).alias("welcome5_avg_product_id"),
    )
    .filter(F.col("avg_product_id") > 0)
    .orderBy(F.col("product_id").desc_nulls_last())
)
