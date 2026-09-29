from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("row_num", "id")
w3 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lower("status").alias("status_lower"),
        F.lower("carrier").alias("carrier_lower"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.lead("cost").over(w2).alias("next_cost"),
        F.lag("cost").over(w2).alias("prev_cost"),
        F.row_number().over(w2).alias("seq_no"),
        F.sum("cost").over(w3).alias("cumulative_cost"),
    )
)

enriched = (
    with_windows
    .filter(F.year("delivered_date") == 2023)
    .withColumn("running_avg_cumulative_cost", F.avg("cumulative_cost").over(w4))
    .withColumn("running_min_cumulative_cost", F.min("cumulative_cost").over(w4))
    .withColumn("running_max_cumulative_cost", F.max("cumulative_cost").over(w4))
    .withColumn("running_count_cumulative_cost", F.count("cumulative_cost").over(w4))
)

result = (
    enriched
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "carrier_filled",
        "row_num",
        "next_cost",
        "prev_cost",
        F.when(F.col("carrier").rlike("^(Kerry|DHL)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("carrier_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
