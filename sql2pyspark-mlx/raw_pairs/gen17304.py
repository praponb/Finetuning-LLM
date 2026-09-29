from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("last3_max_cost").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("last3_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("shipped_date"), "carrier")
w6 = Window.partitionBy("status", "order_id").orderBy("cost", "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("last3_min_cost").desc(), "id")
w8 = Window.partitionBy("order_id", "status").orderBy("dedup_rank", "id")
w9 = Window.partitionBy("cost_avg_3_tier").orderBy(F.col("num_status").desc())

metrics = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-11-01")
    .withColumn("last3_max_cost", F.max("cost").over(w_recent))
    .withColumn("last3_count_cost", F.count("cost").over(w_recent))
    .withColumn("last3_min_cost", F.min("cost").over(w_recent))
    .withColumn("last3_sum_cost", F.sum("cost").over(w_recent))
)

metrics_shipments = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w2).alias("last3_max_cost_dense_rank"),
        F.avg("cost").over(w3).alias("cost_avg_3"),
        F.last("last3_min_cost", ignorenulls=True).over(w4).alias("last_known_last3_min_cost"),
        F.round((F.col("last3_max_cost") / F.sum("last3_max_cost").over(w5)), 4).alias("last3_max_cost_share"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

latest = (
    metrics_shipments
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-07-11"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "last3_max_cost",
        "last3_count_cost",
        "last3_min_cost",
        "last3_max_cost_dense_rank",
        "cost_avg_3",
        "last3_max_cost_share",
        "dedup_rank",
        F.col("last3_max_cost_share").cast("int").alias("last3_max_cost_share_int"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank_2", F.row_number().over(w8))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("cost_avg_3") >= 91, "PREMIUM").otherwise("STANDARD").alias("cost_avg_3_tier"),
        F.date_trunc("week", "delivered_date").alias("week_start"),
    )
    .agg(
        F.round(F.sum("last3_max_cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("last3_max_cost_per_delivered"),
        F.avg(F.when(F.col("status") == "LOST", F.col("last3_max_cost_share_int"))).alias("lost_avg_last3_max_cost_share_int"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .withColumn("num_status_percent_rank", F.percent_rank().over(w9))
)
