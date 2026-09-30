from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("created_at") < "2023-12-01")
    .select(
        "id",
        "created_at",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .cube(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.max("num_attributes").alias("max_num_attributes"),
        F.percentile_approx("num_attributes", 0.5).alias("num_attributes_p50"),
    )
    .filter(F.col("max_num_attributes") > 0)
    .orderBy(F.col("hour_window").desc())
)
