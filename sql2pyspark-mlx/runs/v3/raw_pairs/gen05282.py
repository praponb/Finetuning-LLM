from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2025) & (F.col("carrier") == "UPS"))
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") > 68)
    .drop("min_cost")
    .orderBy(F.col("order_id").desc())
)
