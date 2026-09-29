from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("new_group_flag").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy(F.col("sale_date_period").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("island_id", "id")
w6 = Window.partitionBy("week_start").orderBy(F.col("event_count").desc())

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-20")))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
        F.col("units").cast("string").alias("units_num"),
    )
)

cleaned = (
    base
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-06-01"))) & (F.col("revenue") >= 2000))
    .select(
        "*",
        F.col("units").cast("double").alias("units_num_2"),
        F.ceil("revenue").alias("revenue_whole"),
    )
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("sale_date_period").over(w).isNull() | (F.col("sale_date_period") > F.lag("sale_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    islands
    .withColumn("new_group_flag_cume_dist", F.cume_dist().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("prev_revenue", F.lag("revenue", 1).over(w5))
)

grouped = (
    scored
    .groupBy(
        F.date_trunc("week", "sale_date_period").alias("week_start"),
        F.date_trunc("quarter", "sale_date_period").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("new_group_flag_cume_dist", [0.25, 0.5, 0.75]).alias("new_group_flag_cume_dist_quartiles"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("duration_min_2"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .withColumn("event_count_rank", F.rank().over(w6))
)

result = (
    grouped
    .select(
        "week_start",
        "quarter_start",
        "duration_min",
        "distinct_store_ids",
        "event_count_rank",
    )
    .orderBy(F.col("duration_min").desc_nulls_first())
)
