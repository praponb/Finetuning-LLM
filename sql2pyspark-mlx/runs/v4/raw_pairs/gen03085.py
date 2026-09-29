from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("fashion_events").desc())

result = (
    df_products
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.when(F.col("stock_qty") >= 424, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
    )
    .withColumn("fashion_events_rank", F.rank().over(w))
    .orderBy("stock_qty_bit_or")
)
