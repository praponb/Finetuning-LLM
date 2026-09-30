from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([7, 15, 21, 39]))
    .select(
        "id",
        "customer_id",
        "items",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    unpacked
    .withColumn("bulk_lines_tier", F.when(F.col("bulk_lines") >= 3, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["bulk_lines_tier", "customer_id"], ["bulk_lines_tier"], []],
        "bulk_lines_tier",
        "customer_id",
    )
    .agg(
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50_2"),
    )
    .filter(F.col("bulk_lines_p50") >= 0)
)
