from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").asc_nulls_first(), "id")
w_grp = Window.partitionBy("status").orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("cost_rounded", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("status").orderBy("delivered_date_period", "id")
w5 = Window.partitionBy("status").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-11").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_date_period"),
        F.round("cost", 1).alias("cost_rounded"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.sum("cost_rounded").over(w_grp).alias("running_sum_cost_rounded"),
        F.max("cost_rounded").over(w_grp).alias("running_max_cost_rounded"),
        F.avg("cost_rounded").over(w_grp).alias("running_avg_cost_rounded"),
        F.avg("cost").over(w3).alias("cost_avg_5_prior"),
    )
)

gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "cost",
        "status",
        "delivered_date_period",
        "running_sum_cost_rounded",
        F.sum("is_new_session").over(w5).alias("island_id"),
    )
)

result = (
    sessionized
    .select("id", "cost", "delivered_date_period", "running_sum_cost_rounded")
    .orderBy(F.col("delivered_date_period").desc())
)
