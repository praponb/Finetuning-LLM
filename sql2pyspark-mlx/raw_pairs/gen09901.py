from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "items",
        "attributes",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    flattened_attrs
    .withColumn("items_total_tier", F.when(F.col("items_total") >= 1430, "HIGH").otherwise("LOW"))
    .withColumn("max_item_price_tier", F.when(F.col("max_item_price") >= 218, "PREMIUM").otherwise("STANDARD"))
    .withColumn("bulk_lines_tier", F.when(F.col("bulk_lines") >= 2, "LARGE").otherwise("SMALL"))
    .rollup("items_total_tier", "max_item_price_tier", "bulk_lines_tier")
    .agg(
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
        F.sum("max_item_price").alias("total_max_item_price"),
        F.grouping("items_total_tier").alias("grouping_level"),
    )
    .filter(F.col("avg_bulk_lines") >= 0)
    .orderBy(F.col("max_item_price_tier").desc_nulls_first())
)
