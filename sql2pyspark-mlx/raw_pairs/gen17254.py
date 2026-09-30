from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("units", "id")
w4 = Window.orderBy(F.col("units").desc_nulls_last())
w_running = Window.orderBy("units_dense_rank", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-21").cast("date"))
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
    .withColumn("revenue_band", F.when(F.col("revenue") >= 3100, "High").when(F.col("revenue") >= 800, "Medium").otherwise("Low"))
)

scored = (
    filtered
    .withColumn("last4_sum_units", F.sum("units").over(w_grp))
    .withColumn("last4_min_units", F.min("units").over(w_grp))
    .withColumn("last4_count_units", F.count("units").over(w_grp))
)

filtered_store_sales = (
    scored
    .filter(F.col("sale_date") >= F.lit("2023-03-15").cast("date"))
    .select(
        "id",
        "units",
        "revenue",
        "revenue_per_unit",
        "revenue_band",
        F.round((F.col("last4_count_units") * 1.15), 2).alias("adjusted_last4_count_units"),
        F.col("units").cast("string").alias("units_num"),
    )
)

metrics = (
    filtered_store_sales
    .withColumn("cumulative_adjusted_last4_count_units", F.sum("adjusted_last4_count_units").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("units_dense_rank", F.dense_rank().over(w4))
)

cleaned = (
    metrics
    .filter(F.col("revenue") >= 3100)
    .select(
        "id",
        "revenue",
        "revenue_band",
        "adjusted_last4_count_units",
        "units_num",
        "cumulative_adjusted_last4_count_units",
        "seq_no",
        "units_dense_rank",
        (F.col("cumulative_adjusted_last4_count_units") * F.col("seq_no")).alias("cumulative_adjusted_last4_count_units_times_seq_no"),
    )
)

scored_store_sales = (
    cleaned
    .select(
        "*",
        F.lead("revenue").over(w_running).alias("next_revenue"),
        F.lead("units_dense_rank").over(w_running).alias("next_units_dense_rank"),
        F.lag("units_dense_rank").over(w_running).alias("prev_units_dense_rank"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

cleaned_store_sales = (
    scored_store_sales
    .filter(F.col("row_seq").between(4, 6))
    .select(
        "id",
        "revenue_band",
        "units_num",
        "cumulative_adjusted_last4_count_units",
        "cumulative_adjusted_last4_count_units_times_seq_no",
        "next_revenue",
        "prev_units_dense_rank",
        "row_seq",
        (F.col("revenue") + F.col("adjusted_last4_count_units")).alias("revenue_plus_adjusted_last4_count_units"),
    )
)

result = (
    cleaned_store_sales
    .groupBy(
        F.when(F.col("prev_units_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_units_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.percentile_approx("prev_units_dense_rank", [0.25, 0.5, 0.75]).alias("prev_units_dense_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_units_dense_rank", [0.25, 0.5, 0.75]).alias("prev_units_dense_rank_quartiles_2"),
    )
)
