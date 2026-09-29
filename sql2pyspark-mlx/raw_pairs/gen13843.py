from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w3 = Window.partitionBy("running_sum_cost_tier").orderBy(F.col("flash_avg_running_sum_cost").desc())

base = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-04-22").cast("date"))
    .select(
        "*",
        F.lower("carrier").alias("carrier_lower"),
        F.concat_ws(",", "carrier", "status").alias("carrier_status_key"),
    )
)

scored = (
    base
    .withColumn("running_count_cost", F.count("cost").over(w_running))
    .withColumn("running_max_cost", F.max("cost").over(w_running))
    .withColumn("running_min_cost", F.min("cost").over(w_running))
    .withColumn("running_sum_cost", F.sum("cost").over(w_running))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest
    .filter((F.col("delivered_date") >= F.lit("2023-05-25").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "carrier_lower",
        "carrier_status_key",
        "running_count_cost",
        "running_max_cost",
        "running_min_cost",
        "running_sum_cost",
        "row_num",
        (F.col("row_num") - F.col("running_count_cost")).alias("row_num_minus_running_count_cost"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.discount"), F.col("d.order_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("running_sum_cost") >= 500, "PREMIUM").otherwise("STANDARD").alias("running_sum_cost_tier"),
        "order_id",
        F.when(F.col("cost") >= 104, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.avg(F.when(F.col("carrier") == "UPS", F.col("running_count_cost"))).alias("ups_avg_running_count_cost"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("discount_per_kerry"),
        F.avg(F.when(F.col("carrier") == "Flash", F.col("running_sum_cost"))).alias("flash_avg_running_sum_cost"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
    )
    .withColumn("flash_avg_running_sum_cost_rank", F.rank().over(w3))
    .orderBy(F.col("dhl_count").asc_nulls_last())
)
