from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "attributes",
        F.size("items").alias("num_items"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)
