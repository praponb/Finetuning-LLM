from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("cost_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("carrier").orderBy(F.col("cost_share").asc_nulls_last(), "id")
w5 = Window.partitionBy("order_id", "status").orderBy("cost_share", "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

valid = (
    latest
    .filter((F.col("delivered_date") >= F.lit("2023-06-15").cast("date")) & (F.col("cost") >= 57))
    .withColumn("cost_whole", F.floor("cost"))
)

enriched = (
    valid
    .select(
        "*",
        F.first("cost_whole", ignorenulls=True).over(w2).alias("first_known_cost_whole"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    deduped
    .withColumn("rn_2", F.row_number().over(w5))
    .filter(F.col("rn_2") == 1)
)

result = (
    ranked
    .withColumn("activity_month", F.date_trunc("MONTH", "shipped_date"))
    .withColumn("cost_share_tier", F.when(F.col("cost_share") >= 0.87, "LARGE").otherwise("SMALL"))
    .rollup("activity_month", "cost_share_tier")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("first_known_cost_whole")).otherwise(0)).alias("flash_first_known_cost_whole"),
        F.grouping("activity_month").alias("gid"),
    )
)
