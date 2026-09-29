from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("cost")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w5 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id", "carrier").orderBy(F.col("is_kerry").desc_nulls_last())
w7 = Window.partitionBy("order_id", "carrier")
w8 = Window.partitionBy("order_id", "carrier").orderBy(F.col("is_kerry").desc_nulls_last(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2024-03-05").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("carrier") == "Kerry", 1).otherwise(0).alias("is_kerry"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

enriched = (
    cleaned
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w2))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("next_is_kerry", F.lead("is_kerry", 2).over(w4))
    .withColumn("running_is_kerry", F.sum("is_kerry").over(w5))
)

cleaned_shipments = (
    enriched
    .filter(F.year("delivered_date") >= 2023)
    .withColumn("prev_cost_filled", F.coalesce("prev_cost", F.lit(0)))
    .withColumn("carrier_filled_2", F.coalesce("carrier", F.lit("UNKNOWN")))
)

scored = (
    cleaned_shipments
    .select(
        "*",
        F.rank().over(w6).alias("is_kerry_rank"),
        F.round((F.col("prev_cost") / F.sum("prev_cost").over(w7)), 4).alias("prev_cost_share"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("prev_cost_filled") >= 82, "PREMIUM").otherwise("STANDARD").alias("prev_cost_filled_tier"),
        F.when(F.col("prev_cost_share") >= 0.64, "HIGH").otherwise("LOW").alias("prev_cost_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.avg("cost_share").alias("avg_cost_share"),
    )
    .orderBy("kerry_count")
)
