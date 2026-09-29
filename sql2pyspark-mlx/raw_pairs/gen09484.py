from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    lines
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets([["activity_day", "tag"], ["activity_day"]], "activity_day", "tag")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .orderBy(F.col("bundle_rows").desc())
)
