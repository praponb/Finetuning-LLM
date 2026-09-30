from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quantity_adjusted_tier").orderBy(F.col("line_total_p95").desc())

valid = (
    df_order_items
    .filter(F.col("order_id").isin([10, 36]))
    .select(
        "*",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_adjusted"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity_adjusted") >= 86, "PREMIUM").otherwise("STANDARD").alias("quantity_adjusted_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("line_total", 0.95).alias("line_total_p95"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("line_total", 0.95).alias("line_total_p95_2"),
    )
    .filter(F.col("quantity_stddev") < 0)
    .withColumn("line_total_p95_dense_rank", F.dense_rank().over(w))
)
