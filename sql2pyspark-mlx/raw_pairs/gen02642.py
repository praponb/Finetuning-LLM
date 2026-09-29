from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("created_at").alias("first_created_at"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
