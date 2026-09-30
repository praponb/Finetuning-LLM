from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy(F.year("opened_date")).orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy("region_count_sqft", "id")
w5 = Window.partitionBy("region").orderBy("prev_sqft", "id")
w6 = Window.partitionBy("region").orderBy(F.col("first_known_region_count_sqft").desc_nulls_last())
w_running = Window.partitionBy("region").orderBy("prev_sqft", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("region").orderBy(F.col("next_sqft").desc(), "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

scored = (
    df_stores
    .select(
        "*",
        F.lag("sqft", 2, 0).over(w).alias("prev_sqft"),
        F.count("sqft").over(w2).alias("region_count_sqft"),
    )
)

scored_stores = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "prev_sqft",
        "region_count_sqft",
        F.first("region_count_sqft", ignorenulls=True).over(w3).alias("first_known_region_count_sqft"),
        F.lead("sqft", 1).over(w4).alias("next_sqft"),
    )
)

with_windows = (
    scored_stores
    .select(
        "*",
        F.lag("first_known_region_count_sqft").over(w5).alias("prev_first_known_region_count_sqft"),
        F.lead("first_known_region_count_sqft").over(w5).alias("next_first_known_region_count_sqft"),
        F.lag("sqft").over(w5).alias("prev_sqft_2"),
        F.lead("sqft").over(w5).alias("next_sqft_2"),
        F.row_number().over(w5).alias("row_seq"),
        F.rank().over(w6).alias("first_known_region_count_sqft_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "sqft",
        "prev_sqft",
        "region_count_sqft",
        "first_known_region_count_sqft",
        "next_first_known_region_count_sqft",
        "prev_sqft_2",
        "next_sqft_2",
        "row_seq",
        F.max("next_sqft").over(w_running).alias("last4_max_next_sqft"),
        F.min("next_sqft").over(w_running).alias("last4_min_next_sqft"),
        F.count("next_sqft").over(w_running).alias("last4_count_next_sqft"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

result = (
    latest_match
    .select(
        "id",
        "sqft",
        "prev_sqft",
        "region_count_sqft",
        "first_known_region_count_sqft",
        "next_first_known_region_count_sqft",
        "prev_sqft_2",
        "row_seq",
        "last4_count_next_sqft",
    )
    .orderBy(F.col("last4_count_next_sqft").desc())
)
