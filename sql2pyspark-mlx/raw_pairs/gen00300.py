from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "PREMIUM").otherwise("STANDARD").alias("bulk_lines_tier"),
    )
    .agg(
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy("bulk_lines_tier")
)
