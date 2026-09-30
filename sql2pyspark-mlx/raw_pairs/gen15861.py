from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    flattened
    .groupBy("tag", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy(F.col("bundle_rows").asc_nulls_last())
)
