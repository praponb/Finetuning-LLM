from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost_rounded", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_rounded").asc_nulls_first())
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w4 = Window.partitionBy("carrier").orderBy("cost", "id")
w5 = Window.partitionBy("carrier").orderBy("cost_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy(F.col("cost_avg_2").desc(), "id")

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("carrier") == "Kerry"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

base = (
    valid
    .filter((F.col("delivered_date") > F.to_date(F.lit("2023-06-11"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status_prefix",
        "carrier_filled",
        F.round((F.col("cost") * 0.85), 2).alias("cost_scaled"),
        F.round("cost", 0).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

metrics = (
    base
    .withColumn("cost_avg_2", F.avg("cost").over(w))
    .withColumn("cost_rounded_rank", F.rank().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("first_known_cost_rounded", F.first("cost_rounded", ignorenulls=True).over(w5))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "delivered_date",
        "cost",
        "status_prefix",
        "cost_scaled",
        "cost_rounded",
        "carrier_clean",
        "cost_avg_2",
    )
    .orderBy(F.col("cost").desc_nulls_last())
)
