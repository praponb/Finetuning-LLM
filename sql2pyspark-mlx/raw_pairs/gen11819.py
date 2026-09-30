from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "status").orderBy("shipped_date_plus_24d", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status", "order_id").orderBy("last7_avg_cost", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w5 = Window.partitionBy("order_id").orderBy("last7_max_cost", "id")
w6 = Window.partitionBy("order_id").orderBy("last7_max_cost_adjusted", "id")

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-07-30").cast("date"))
    .select(
        "*",
        F.add_months("delivered_date", 1).alias("delivered_date_plus_months"),
        F.date_add("shipped_date", 24).alias("shipped_date_plus_24d"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

cleaned = (
    base
    .filter(F.col("order_id") == 39)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_plus_24d",
        "carrier_clean",
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("cost").over(w_grp).alias("last7_avg_cost"),
        F.max("cost").over(w_grp).alias("last7_max_cost"),
        F.count("cost").over(w_grp).alias("last7_count_cost"),
        F.sum("cost").over(w_grp).alias("last7_sum_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "shipped_date_plus_24d",
        "last7_max_cost",
        "last7_sum_cost",
        "cost_share",
        F.avg("last7_avg_cost").over(w3).alias("last3_avg_last7_avg_cost"),
        F.max("last7_avg_cost").over(w3).alias("last3_max_last7_avg_cost"),
        F.min("last7_avg_cost").over(w3).alias("last3_min_last7_avg_cost"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    deduped
    .filter(F.col("delivered_date") > F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "last7_max_cost",
        "cost_share",
        "last3_avg_last7_avg_cost",
        "last3_max_last7_avg_cost",
        "last3_min_last7_avg_cost",
        F.round((F.col("last7_max_cost") * 0.9), 2).alias("last7_max_cost_adjusted"),
        F.coalesce("last7_max_cost", F.lit(0)).alias("last7_max_cost_filled"),
    )
)

scored_shipments = (
    valid
    .select(
        "*",
        F.lag("last7_max_cost_filled").over(w5).alias("prev_last7_max_cost_filled"),
        F.lead("last3_avg_last7_avg_cost").over(w5).alias("next_last3_avg_last7_avg_cost"),
        F.lag("last3_avg_last7_avg_cost").over(w5).alias("prev_last3_avg_last7_avg_cost"),
        F.row_number().over(w5).alias("event_seq"),
        F.lead("last3_max_last7_avg_cost", 1).over(w6).alias("next_last3_max_last7_avg_cost"),
    )
)

result = (
    scored_shipments
    .select(
        "id",
        "delivered_date",
        "last7_max_cost",
        "cost_share",
        "last3_avg_last7_avg_cost",
        "last3_min_last7_avg_cost",
        "prev_last7_max_cost_filled",
        "next_last3_avg_last7_avg_cost",
        "prev_last3_avg_last7_avg_cost",
    )
    .orderBy(F.col("last7_max_cost").desc())
)
