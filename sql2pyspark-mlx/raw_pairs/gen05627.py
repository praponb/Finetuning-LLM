from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("num_attributes_tier").orderBy(F.col("avg_bulk_lines").desc())

unpacked = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_attributes") >= 84, "LARGE").otherwise("SMALL").alias("num_attributes_tier"),
        F.when(F.col("bulk_lines") >= 3, "PREMIUM").otherwise("STANDARD").alias("bulk_lines_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("bulk_lines", [0.25, 0.5, 0.75]).alias("bulk_lines_quartiles"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.percentile_approx("num_attributes", [0.25, 0.5, 0.75]).alias("num_attributes_quartiles"),
        F.percentile_approx("num_attributes", 0.5).alias("num_attributes_p50"),
    )
    .filter(F.col("avg_bulk_lines") > 0)
    .withColumn("avg_bulk_lines_percent_rank", F.percent_rank().over(w))
)
