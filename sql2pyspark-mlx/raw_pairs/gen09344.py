from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("is_kerry").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy("is_kerry", "id")
w3 = Window.orderBy("delivered_date", "id")
w4 = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy("cumulative_is_kerry", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(-5, -1)

filtered = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "FedEx", "DHL"]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("shipped_date") > F.lit("2023-03-20").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_clean",
        F.when(F.col("carrier") == "Kerry", 1).otherwise(0).alias("is_kerry"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w2))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
    .withColumn("cumulative_is_kerry", F.sum("is_kerry").over(w4))
)

enriched = (
    scored
    .filter(F.col("status") == "IN_TRANSIT")
    .withColumn("last_known_next_cost", F.last("next_cost", ignorenulls=True).over(w_ordered))
    .withColumn("next_cost_avg_5_prior", F.avg("next_cost").over(w6))
)

result = (
    enriched
    .select("id", "carrier", "cost", "last_known_next_cost", "next_cost", "prev_cost")
    .unpivot(
        ids=["id", "carrier"],
        values=["cost", "last_known_next_cost", "next_cost", "prev_cost"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
