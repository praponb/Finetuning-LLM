from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .withColumn("price_tier", F.when(F.col("price") >= 200, "LARGE").otherwise("SMALL"))
    .rollup("price_tier")
    .agg(
        F.count("*").alias("num_rows"),
        F.max("price").alias("max_price"),
        F.grouping("price_tier").alias("grouping_id"),
    )
    .filter(F.col("num_rows") < 1)
    .orderBy(F.col("num_rows").asc_nulls_first())
)
