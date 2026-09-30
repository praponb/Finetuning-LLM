from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w_ordered = Window.partitionBy("city").orderBy("sqft")
w3 = Window.partitionBy("region").orderBy("sqft_cume_dist", "id")
w4 = Window.partitionBy("region", "city").orderBy("sqft_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("sqft_cume_dist_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-22").cast("date")) & F.col("store_name").like("Riv%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper("region").alias("region_upper"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "region_upper",
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w_ordered).alias("sqft_cume_dist"),
    )
)

enriched_stores = (
    enriched
    .filter(F.col("sqft_cume_dist") < 0.23)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "region_upper",
        "event_seq",
        "sqft_cume_dist",
        F.ntile(3).over(w3).alias("sqft_cume_dist_bucket"),
        F.avg("event_seq").over(w4).alias("event_seq_avg_6"),
    )
)

eligible = (
    enriched_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") > "2025-02-01"), "left_anti")
)

enriched_stores_2 = (
    eligible
    .withColumn("running_count_sqft_cume_dist_bucket", F.count("sqft_cume_dist_bucket").over(w5))
    .withColumn("running_avg_sqft_cume_dist_bucket", F.avg("sqft_cume_dist_bucket").over(w5))
    .withColumn("running_max_sqft_cume_dist_bucket", F.max("sqft_cume_dist_bucket").over(w5))
    .withColumn("running_sum_sqft_cume_dist_bucket", F.sum("sqft_cume_dist_bucket").over(w5))
    .withColumn("running_event_seq", F.sum("event_seq").over(w6))
)

result = (
    enriched_stores_2
    .groupBy(
        F.when(F.col("running_event_seq") >= 39, "PREMIUM").otherwise("STANDARD").alias("running_event_seq_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.round(F.avg("event_seq_avg_6"), 2).alias("avg_event_seq_avg_6"),
    )
    .filter(F.col("avg_event_seq_avg_6") > 0)
    .orderBy(F.col("avg_event_seq_avg_6").asc_nulls_first())
)
