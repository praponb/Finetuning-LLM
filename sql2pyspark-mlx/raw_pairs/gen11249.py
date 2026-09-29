from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "items",
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.size("attributes").alias("num_attributes"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("basket_amount") >= 1780, "HIGH").otherwise("LOW").alias("basket_amount_tier"),
        F.when(F.col("num_attributes") >= 68, "LARGE").otherwise("SMALL").alias("num_attributes_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("basket_amount", 0.5).alias("basket_amount_p50"),
        F.avg("num_attributes").alias("avg_num_attributes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("basket_amount_p50") > 0)
    .orderBy(F.col("num_attributes_tier").asc_nulls_last())
)
