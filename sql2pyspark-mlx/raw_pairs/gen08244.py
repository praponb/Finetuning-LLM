from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets([["promo", "activity_day"], ["promo"]], "promo", "activity_day")
    .agg(
        F.max(F.when(F.col("promo") == "BUNDLE", F.col("qty"))).alias("bundle_max_qty"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.max("created_at").alias("last_created_at"),
    )
    .filter(F.col("bundle_max_qty") > 0)
    .orderBy("activity_day")
)
