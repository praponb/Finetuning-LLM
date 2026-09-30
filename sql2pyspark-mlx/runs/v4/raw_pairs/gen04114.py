from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("quantity_avg_5_tier").orderBy(F.col("row_count").desc())

scored = (
    df_inventory
    .select(
        "*",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("quantity_avg_5") >= 235, "HIGH").otherwise("LOW").alias("quantity_avg_5_tier"),
        F.when(F.col("quantity_percent_rank") >= 0.84, "PREMIUM").otherwise("STANDARD").alias("quantity_percent_rank_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity_percent_rank", [0.25, 0.5, 0.75]).alias("quantity_percent_rank_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity_avg_5", [0.25, 0.5, 0.75]).alias("quantity_avg_5_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .withColumn("row_count_rank", F.rank().over(w3))
    .orderBy(F.col("distinct_store_ids").desc())
)
