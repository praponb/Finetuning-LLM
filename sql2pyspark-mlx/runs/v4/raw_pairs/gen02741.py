from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "PREMIUM").otherwise("STANDARD").alias("bulk_lines_tier"),
    )
    .agg(
        F.min("created_at").alias("started_at"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
    )
)
