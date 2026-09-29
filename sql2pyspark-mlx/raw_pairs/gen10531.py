from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("shipped_date_day_rounded").desc())
w_running_2 = Window.partitionBy("carrier").orderBy("shipped_date_day", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("status").orderBy(F.col("shipped_date_day_rounded_dense_rank").desc(), "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-08-03"))) & (F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.dayofmonth("shipped_date").alias("shipped_date_day"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

cleaned = (
    valid
    .filter((F.col("shipped_date_day") == 21) & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "*",
        F.round("shipped_date_day", 1).alias("shipped_date_day_rounded"),
        (F.col("cost") - F.col("shipped_date_day")).alias("cost_minus_shipped_date_day"),
    )
)

metrics = (
    cleaned
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "carrier_clean",
        "shipped_date_day_rounded",
        F.dense_rank().over(w_running).alias("shipped_date_day_rounded_dense_rank"),
        F.avg("cost_minus_shipped_date_day").over(w_running_2).alias("cost_minus_shipped_date_day_avg_3"),
        F.cume_dist().over(w3).alias("cost_cume_dist"),
    )
)

base = (
    metrics
    .filter(F.col("cost_cume_dist").isNull())
    .select("*", F.substring("status", 1, 4).alias("status_prefix"))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "shipped_date_day_rounded", "cost_cume_dist")
)

result = (
    deduped
    .select("id", "shipped_date_day_rounded", "cost_cume_dist")
    .unpivot(
        ids=["id"],
        values=["shipped_date_day_rounded", "cost_cume_dist"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
