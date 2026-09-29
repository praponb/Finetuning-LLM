from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("carrier", "status").orderBy(F.col("cost_dense_rank").desc_nulls_last())
w6 = Window.partitionBy(F.date_trunc("MONTH", "shipped_date"), "carrier").orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("carrier", "status").orderBy("running_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .withColumn("running_cost", F.sum("cost").over(w2))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
    .withColumn("cost_dense_rank", F.dense_rank().over(w4))
)

valid = (
    scored
    .filter((F.col("shipped_date") >= F.lit("2023-09-21").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "rn",
        "running_cost",
        "next_cost",
        "cost_dense_rank",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.percent_rank().over(w5).alias("cost_dense_rank_percent_rank"),
        F.avg("weeks_shipped_date_to_delivered_date").over(w6).alias("weeks_shipped_date_to_delivered_date_avg_5"),
        F.last("next_cost", ignorenulls=True).over(w7).alias("last_known_next_cost"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cost") >= 84, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.count("*").alias("cnt"),
    )
)
