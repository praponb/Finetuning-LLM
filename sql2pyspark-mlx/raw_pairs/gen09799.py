from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["gift"].alias("gift"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    parsed
    .withColumn("bulk_lines_tier", F.when(F.col("bulk_lines") >= 3, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["bulk_lines_tier", "customer_id"], ["bulk_lines_tier"], []],
        "bulk_lines_tier",
        "customer_id",
    )
    .agg(
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.grouping("bulk_lines_tier").alias("grouping_id"),
    )
    .orderBy(F.col("avg_bulk_lines").desc())
)
