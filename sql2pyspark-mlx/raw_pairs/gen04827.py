from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy("corr_units_revenue_per_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-07").cast("date"))
    .withColumn("revenue_scaled", F.round((F.col("revenue") * 100), 2))
)

with_windows = (
    filtered
    .select(
        "*",
        F.stddev_samp("revenue").over(w).alias("store_id_stddev_samp_revenue"),
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

base = (
    with_windows
    .filter(F.col("store_id_stddev_samp_revenue") <= 3724)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_scaled",
        "store_id_stddev_samp_revenue",
        "event_seq",
        F.round((F.col("corr_units_revenue") / F.col("units")), 2).alias("corr_units_revenue_per_units"),
        F.round((F.col("revenue") * 1.15), 2).alias("revenue_adjusted"),
    )
)

scored = (
    base
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_scaled",
        "store_id_stddev_samp_revenue",
        "event_seq",
        F.rank().over(w3).alias("units_rank"),
        F.first("store_id_stddev_samp_revenue", ignorenulls=True).over(w4).alias("first_known_store_id_stddev_samp_revenue"),
        F.avg("revenue_scaled").over(w5).alias("product_id_avg_revenue_scaled"),
        F.sum("revenue_adjusted").over(w6).alias("cumulative_revenue_adjusted"),
        F.last("revenue_adjusted", ignorenulls=True).over(w7).alias("last_known_revenue_adjusted"),
    )
)

result = (
    scored
    .groupBy("product_id")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("units_rank", [0.25, 0.5, 0.75]).alias("units_rank_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("units_bit_and").desc())
)
