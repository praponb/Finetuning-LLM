from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date_plus_months", "id")
w_hist = Window.orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("region").orderBy("opened_date_plus_months", "id")
w4 = Window.partitionBy("region").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("region").orderBy("seq_no", "id")
w6 = Window.partitionBy("region", "city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("seq_no_2", "id")
w_recent = Window.partitionBy("city", "region").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region").orderBy(F.col("event_seq").desc(), "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

enriched = (
    metrics
    .withColumn("seq_no_bucket", F.ntile(4).over(w5))
    .withColumn("last_known_seq_no_2", F.last("seq_no_2", ignorenulls=True).over(w6))
    .withColumn("prev_event_seq_2", F.lag("event_seq_2", 1).over(w7))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w_recent))
    .withColumn("seq_no_3", F.row_number().over(w9))
)

eligible = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date_plus_months")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "opened_date_plus_months").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("seq_no")).otherwise(0)).alias("tokyo_seq_no"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("region") == "West", F.col("last_known_seq_no")).otherwise(0)).alias("west_last_known_seq_no"),
    )
    .filter(F.col("tokyo_seq_no") > 0)
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
