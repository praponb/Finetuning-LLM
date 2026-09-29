from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").asc_nulls_first(), "id")
w2 = Window.orderBy("sale_date", "id")
w3 = Window.orderBy(F.col("units_whole").desc_nulls_last())
w_recent = Window.orderBy(F.col("units_whole").desc(), "id")
w5 = Window.orderBy("units_whole", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("YEAR", "sale_date")).orderBy("next_units_whole", "id")
w8 = Window.orderBy("next_units_whole", "id")
w9 = Window.orderBy(F.col("units_whole_dense_rank_avg_2").asc_nulls_last())
w10 = Window.orderBy("prev_next_units_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("units_whole_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-05-23").cast("date")) & F.col("store_id").isin([1, 2, 4, 6]))
    .withColumn("units_whole", F.ceil("units"))
    .withColumn("years_since_sale_date", (F.datediff(F.current_date(), "sale_date") / 365.25))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "sale_date", "units", "units_whole")
)

metrics = (
    latest
    .filter(F.col("units") == 38)
    .select(
        "*",
        F.lead("units_whole", 2).over(w2).alias("next_units_whole"),
        F.dense_rank().over(w3).alias("units_whole_dense_rank"),
    )
)

enriched = (
    metrics
    .withColumn("units_whole_percent_rank", F.percent_rank().over(w3))
    .withColumn("units_whole_bucket", F.ntile(10).over(w_recent))
    .withColumn("units_whole_dense_rank_avg_2", F.avg("units_whole_dense_rank").over(w5))
    .withColumn("cumulative_next_units_whole", F.sum("next_units_whole").over(w6))
    .withColumn("prev_next_units_whole", F.lag("next_units_whole", 2).over(w7))
)

enriched_store_sales = (
    enriched
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("units_whole_dense_rank_avg_2_cume_dist", F.cume_dist().over(w9))
    .withColumn("last_known_prev_next_units_whole", F.last("prev_next_units_whole", ignorenulls=True).over(w10))
)

enriched_store_sales_2 = (
    enriched_store_sales
    .select(
        "id",
        "units",
        "next_units_whole",
        "units_whole_dense_rank",
        "units_whole_dense_rank_avg_2",
        "cumulative_next_units_whole",
        "prev_next_units_whole",
        "event_seq",
        "units_whole_dense_rank_avg_2_cume_dist",
        "last_known_prev_next_units_whole",
        F.max("last_known_prev_next_units_whole").over(w_running).alias("running_max_last_known_prev_next_units_whole"),
        F.count("last_known_prev_next_units_whole").over(w_running).alias("running_count_last_known_prev_next_units_whole"),
        F.sum("last_known_prev_next_units_whole").over(w_running).alias("running_sum_last_known_prev_next_units_whole"),
    )
)

result = (
    enriched_store_sales_2
    .select(
        "id",
        "next_units_whole",
        "units_whole_dense_rank",
        "units_whole_dense_rank_avg_2",
        "prev_next_units_whole",
        "event_seq",
        "running_max_last_known_prev_next_units_whole",
        "running_count_last_known_prev_next_units_whole",
        "running_sum_last_known_prev_next_units_whole",
    )
)
