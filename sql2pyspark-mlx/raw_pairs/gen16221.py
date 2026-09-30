from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("month", "last_restock_period"), "region").orderBy("burst_id", "id")
w4 = Window.partitionBy(F.year("last_restock_period"), "region").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("burst_id", "id")
w6 = Window.partitionBy("region").orderBy(F.col("burst_id_bucket").desc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-02-28"))) & (F.col("quantity") >= 81))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.last_restock_period",
        F.col("b.store_name"),
        F.col("b.sqft"),
        F.col("b.region"),
    )
)

with_prev = joined.withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "store_id",
        "last_restock_period",
        "store_name",
        "region",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

enriched = (
    islands
    .select(
        "id",
        "last_restock_period",
        "region",
        F.ntile(4).over(w3).alias("burst_id_bucket"),
        F.sum("burst_id").over(w4).alias("cumulative_burst_id"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock_period"))
    .groupingSets(
        [["cumulative_burst_id", "quarter_start"], ["cumulative_burst_id"]],
        "cumulative_burst_id",
        "quarter_start",
    )
    .agg(
        F.avg(F.when(F.col("region") == "North", F.col("row_seq"))).alias("north_avg_row_seq"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
    )
    .filter(F.col("north_avg_row_seq") > 0)
    .orderBy(F.col("north_avg_row_seq").asc_nulls_last())
)
