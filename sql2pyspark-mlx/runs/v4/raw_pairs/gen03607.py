from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status")
w2 = Window.partitionBy("carrier", "status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("cost_whole").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w_grp = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("carrier").orderBy(F.col("cost_times_cost_whole").desc(), "id")
w7 = Window.orderBy("cost_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("cost", "id")
w9 = Window.partitionBy("status").orderBy("cost_whole_bucket")
w10 = Window.partitionBy("status").orderBy(F.col("status_sum_cost_whole").desc(), "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2024-05-02").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.ceil("cost").alias("cost_whole"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.sum("cost_whole").over(w_hist).alias("status_sum_cost_whole"),
        F.avg("cost").over(w2).alias("cost_avg_3"),
        F.ntile(10).over(w3).alias("cost_whole_bucket"),
    )
)

valid = (
    enriched
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-11-13"))) & F.col("carrier").isNotNull())
    .select(
        "*",
        (F.col("cost_whole_bucket") * F.col("cost")).alias("cost_whole_bucket_times_cost"),
        F.when(F.col("cost") >= 106, "High").when(F.col("cost") >= 42, "Medium").otherwise("Low").alias("cost_tier"),
    )
)

valid_shipments = (
    valid
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        (F.col("cost") * F.col("cost_whole")).alias("cost_times_cost_whole"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

ranked = (
    valid_shipments
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("cost_avg_3").over(w_grp).alias("running_cost_avg_3"),
        F.ntile(5).over(w_grp_2).alias("cost_times_cost_whole_bucket"),
        F.sum("cost_times_cost_whole").over(w7).alias("running_cost_times_cost_whole"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.rank().over(w9).alias("cost_whole_bucket_rank"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "cost_whole",
        "status_sum_cost_whole",
        "cost_avg_3",
        "cost_whole_bucket",
        "cost_whole_bucket_times_cost",
        F.when(F.col("status").rlike("^(LOST|DELIVERED)$"), "EXTENDED").when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy("cost_whole_bucket_times_cost")
)
