from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").asc_nulls_first())
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy(F.date_trunc("WEEK", "sale_date")).orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.year("sale_date")).orderBy(F.col("adjusted_units").asc_nulls_first())
w5 = Window.orderBy(F.col("revenue").desc(), "id")
w6 = Window.partitionBy(F.month("sale_date")).orderBy(F.col("next_units").desc(), "id")
w7 = Window.orderBy("adjusted_units", "id")
w8 = Window.orderBy("adjusted_units_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("running_revenue").asc_nulls_last())
w10 = Window.orderBy(F.col("event_seq").desc())

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-08-01")))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") * 1.15), 2).alias("adjusted_units"),
    )
)

metrics = (
    base
    .withColumn("next_units", F.lead("units", 2, 0).over(w2))
    .withColumn("running_revenue", F.sum("revenue").over(w3))
    .withColumn("adjusted_units_dense_rank", F.dense_rank().over(w_part))
)

metrics_store_sales = (
    metrics
    .filter(F.col("units") < 32)
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("event_seq_2", F.row_number().over(w6))
    .withColumn("next_adjusted_units_dense_rank", F.lead("adjusted_units_dense_rank", 2, 0).over(w7))
    .withColumn("cumulative_next_units", F.sum("next_units").over(w8))
)

matched_keys = (
    metrics_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.year("sale_date") >= 2025)
        .select("id")
    )
)

verified = (
    metrics_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_store_sales_2 = (
    verified
    .select(
        "*",
        F.cume_dist().over(w9).alias("running_revenue_cume_dist"),
        F.percent_rank().over(w10).alias("event_seq_percent_rank"),
    )
)

daily = (
    metrics_store_sales_2
    .withColumn("year_start", F.date_trunc("YEAR", "sale_date"))
    .withColumn("adjusted_units_dense_rank_tier", F.when(F.col("adjusted_units_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "adjusted_units_dense_rank_tier"], ["year_start"], []],
        "year_start",
        "adjusted_units_dense_rank_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.round(F.avg("cumulative_next_units"), 2).alias("avg_cumulative_next_units"),
        F.count("*").alias("event_count_2"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select("year_start", "event_count", "cnt", "avg_cumulative_next_units", "gid")
    .orderBy(F.col("cnt").desc(), F.col("event_count").desc())
)
