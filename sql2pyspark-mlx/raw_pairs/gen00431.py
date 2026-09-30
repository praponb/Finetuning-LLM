from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "PREMIUM").otherwise("STANDARD").alias("bulk_lines_tier"),
    )
    .agg(
        F.percentile_approx("bulk_lines", 0.95).alias("bulk_lines_p95"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
    )
    .orderBy(F.col("bulk_lines_p50").desc_nulls_first())
)
