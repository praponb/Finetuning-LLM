from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id")
w3 = Window.orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("category").orderBy("units_avg_3")
w6 = Window.partitionBy(F.year("launch_date"), "category")
w7 = Window.partitionBy(F.year("launch_date"), "category").orderBy(F.col("row_seq").desc_nulls_last())
w8 = Window.orderBy("units_avg_3_avg_3", "id")
w9 = Window.orderBy("row_seq_percent_rank", "id")
w10 = Window.orderBy("units_avg_3_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("units_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("row_seq_times_units_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("running_product_id_max_units_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy(F.col("row_seq_times_units_avg_3").desc(), "id")
w15 = Window.orderBy("prev_units_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.partitionBy("first_known_last_known_row_seq_times_units_avg_3_tier").orderBy(F.col("distinct_ids").desc())

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 8]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.units",
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "category",
        "launch_date",
        F.row_number().over(w).alias("row_seq"),
        F.max("units").over(w2).alias("product_id_max_units"),
        F.avg("units").over(w3).alias("units_avg_3"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "id",
        "row_seq",
        "units_avg_3",
        F.avg("units_avg_3").over(w4).alias("units_avg_3_avg_3"),
        F.rank().over(w5).alias("units_avg_3_rank"),
        F.round((F.col("product_id_max_units") / F.sum("product_id_max_units").over(w6)), 4).alias("product_id_max_units_share"),
        F.percent_rank().over(w7).alias("row_seq_percent_rank"),
    )
)

base = (
    with_windows_store_sales
    .filter(F.col("row_seq_percent_rank") < 0.39)
    .select(
        "id",
        "units_avg_3",
        "units_avg_3_avg_3",
        "units_avg_3_rank",
        "product_id_max_units_share",
        "row_seq_percent_rank",
        F.col("row_seq_percent_rank").cast("int").alias("row_seq_percent_rank_int"),
        (F.col("row_seq") * F.col("units_avg_3")).alias("row_seq_times_units_avg_3"),
    )
)

metrics = (
    base
    .select(
        "id",
        "units_avg_3",
        "product_id_max_units_share",
        "row_seq_percent_rank",
        "row_seq_percent_rank_int",
        "row_seq_times_units_avg_3",
        F.ntile(10).over(w8).alias("units_avg_3_avg_3_bucket"),
        F.lag("units_avg_3", 2).over(w9).alias("prev_units_avg_3"),
        F.sum("units_avg_3_avg_3").over(w10).alias("running_units_avg_3_avg_3"),
        F.last("row_seq_times_units_avg_3", ignorenulls=True).over(w11).alias("last_known_row_seq_times_units_avg_3"),
        F.sum("product_id_max_units_share").over(w12).alias("running_product_id_max_units_share"),
    )
)

enriched_store_sales = (
    metrics
    .withColumn("running_row_seq_percent_rank", F.sum("row_seq_percent_rank").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w14))
    .withColumn("first_known_last_known_row_seq_times_units_avg_3", F.first("last_known_row_seq_times_units_avg_3", ignorenulls=True).over(w15))
)

result = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("first_known_last_known_row_seq_times_units_avg_3") >= 85, "PREMIUM").otherwise("STANDARD").alias("first_known_last_known_row_seq_times_units_avg_3_tier"),
        F.when(F.col("row_seq_percent_rank_int") >= 76, "PREMIUM").otherwise("STANDARD").alias("row_seq_percent_rank_int_tier"),
    )
    .agg(
        F.round(F.avg("units_avg_3_avg_3_bucket"), 2).alias("avg_units_avg_3_avg_3_bucket"),
        F.percentile_approx("product_id_max_units_share", [0.25, 0.5, 0.75]).alias("product_id_max_units_share_quartiles"),
        F.percentile_approx("running_units_avg_3_avg_3", [0.25, 0.5, 0.75]).alias("running_units_avg_3_avg_3_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("cnt"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w16))
    .orderBy(F.col("distinct_ids_2").desc())
)
