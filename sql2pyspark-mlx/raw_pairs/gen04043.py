from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost_filled").desc())
w_grp = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("shipped_date").asc_nulls_first())
w_ordered = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_sub("shipped_date", 57).alias("shipped_date_minus_57d"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

valid_shipments = (
    valid
    .filter((~F.col("carrier").eqNullSafe(F.lit("Flash"))) & F.col("shipped_date").between("2023-10-01", "2025-08-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

with_windows = (
    valid_shipments
    .select(
        "*",
        F.rank().over(w).alias("cost_filled_rank"),
        F.ntile(4).over(w_grp).alias("cost_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "carrier", "shipped_date", "cost_filled", "cost_bucket")
)

scored = (
    latest
    .select(
        "*",
        F.avg("cost_bucket").over(w_ordered).alias("last7_avg_cost_bucket"),
        F.sum("cost_bucket").over(w_ordered).alias("last7_sum_cost_bucket"),
        F.max("cost_bucket").over(w_ordered).alias("last7_max_cost_bucket"),
        F.min("cost_bucket").over(w_ordered).alias("last7_min_cost_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("last7_avg_cost_bucket") >= 6.6, "LARGE").otherwise("SMALL").alias("last7_avg_cost_bucket_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_rows"),
        F.max(F.when(F.col("carrier") == "Flash", F.col("last7_avg_cost_bucket"))).alias("flash_max_last7_avg_cost_bucket"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.avg("last7_min_cost_bucket").alias("avg_last7_min_cost_bucket"),
        F.avg("cost_bucket").alias("avg_cost_bucket"),
    )
    .orderBy("avg_cost_bucket")
)
