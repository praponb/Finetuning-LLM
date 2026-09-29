from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("revenue", "id")
w2 = Window.partitionBy("region").orderBy("sale_date", "id")
w_ordered = Window.partitionBy("region").orderBy("row_seq", "id")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("region").orderBy("region_count_row_seq", "id")

enriched = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        F.col("p.region"),
        F.col("p.store_name"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "sale_date", "region")
)

enriched_store_sales = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

scored = (
    enriched_store_sales
    .select(
        "id",
        "region",
        "event_seq",
        "row_seq",
        F.lag("event_seq", 1).over(w_ordered).alias("prev_event_seq"),
        F.count("row_seq").over(w4).alias("region_count_row_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("region").isNotNull())
    .withColumn("region_prefix", F.substring("region", 1, 3))
)

scored_store_sales = (
    cleaned
    .select(
        "*",
        F.round((F.col("prev_event_seq") / F.sum("prev_event_seq").over(w4)), 4).alias("prev_event_seq_share"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.max("region_count_row_seq").over(w4).alias("region_max_region_count_row_seq"),
    )
)

totals = (
    scored_store_sales
    .groupBy("region")
    .agg(
        F.sum("prev_event_seq").alias("total_prev_event_seq"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.sum(F.when(F.col("region") == "West", F.col("row_seq_2"))).alias("west_total_row_seq_2"),
        F.percentile_approx("prev_event_seq", 0.95).alias("prev_event_seq_p95"),
    )
    .filter(F.col("total_prev_event_seq") < 1000000)
)

result = (
    totals
    .select(
        "region",
        "prev_event_seq_p95",
        F.when(F.col("region").rlike("^(Central|North)$"), "CORE").when(F.col("region").rlike("^(South|East)$"), "EXTENDED").otherwise("PRIORITY").alias("region_zone"),
    )
)
