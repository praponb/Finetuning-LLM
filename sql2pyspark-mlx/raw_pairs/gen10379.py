from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("unit_price_filled_tier").orderBy(F.col("cnt").desc())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("unit_price_filled") >= 79, "LARGE").otherwise("SMALL").alias("unit_price_filled_tier"),
        "product_id",
        F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("unit_price_filled", [0.25, 0.5, 0.75]).alias("unit_price_filled_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
    )
    .filter(F.col("avg_quantity") > 0)
    .withColumn("cnt_percent_rank", F.percent_rank().over(w))
)
