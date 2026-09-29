from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("running_sum_cost").desc())

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-02-17"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.date_format("delivered_date", "MMM yyyy").alias("delivered_date_label"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "order_id",
        "cost",
        "delivered_date_label",
        F.min("cost").over(w).alias("running_min_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "delivered_date_label", "running_min_cost", "running_avg_cost")
)

base_shipments = (
    latest
    .filter(F.col("running_avg_cost") < 39)
    .withColumn("running_min_cost_int", F.col("running_min_cost").cast("int"))
)

result = (
    base_shipments
    .select(
        "id",
        "delivered_date_label",
        "running_min_cost",
        "running_avg_cost",
        "running_min_cost_int",
    )
    .orderBy(F.col("running_min_cost").desc_nulls_last())
)
