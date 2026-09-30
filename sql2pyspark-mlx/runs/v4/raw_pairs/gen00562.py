from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_store_sales
    .select(
        "*",
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.avg("units").over(w2).alias("units_avg_3"),
    )
)

result = (
    with_windows
    .withColumn("units_avg_3_tier", F.when(F.col("units_avg_3") >= 32, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["store_id", "units_avg_3_tier"], ["store_id"]],
        "store_id",
        "units_avg_3_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("units").alias("units_stddev"),
        F.count("*").alias("num_rows_2"),
        F.avg("corr_units_revenue").alias("avg_corr_units_revenue"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("cnt")
)
