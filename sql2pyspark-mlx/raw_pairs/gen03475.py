from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("store_id", "product_id")
w_part = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_inventory
    .select(
        "*",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.count("quantity").over(w2).alias("store_id_count_quantity"),
        F.sum("quantity").over(w_part).alias("running_quantity"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("quantity") >= 227, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("running_quantity", 0.9).alias("running_quantity_p90"),
    )
    .filter(F.col("running_quantity_p90") > 0)
    .orderBy(F.col("quantity_stddev").asc_nulls_last())
)
