from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("cost_adjusted", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy(F.col("cost_adjusted").asc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("order_id_avg_cost_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("delivered_date_plus_61d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("cost_adjusted_cume_dist").desc_nulls_first(), "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w9 = Window.orderBy("cumulative_cost_adjusted_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy(F.col("order_id_avg_cost_adjusted").asc_nulls_last())

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2024-02-18"))) & F.col("order_id").isNotNull() & (F.col("carrier") == "UPS"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        F.date_add("delivered_date", 61).alias("delivered_date_plus_61d"),
        F.round((F.col("cost") * 100), 2).alias("cost_adjusted"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.avg("cost_adjusted").over(w).alias("order_id_avg_cost_adjusted"),
        F.avg("cost_adjusted").over(w2).alias("cost_adjusted_avg_5"),
        F.cume_dist().over(w3).alias("cost_adjusted_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-10-12"))) & F.col("order_id").isNotNull() & (F.col("delivered_date_plus_61d") < "2024-07-01"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date_plus_61d",
        "cost_adjusted",
        "order_id_avg_cost_adjusted",
        "cost_adjusted_avg_5",
        "cost_adjusted_cume_dist",
        F.round("cost_adjusted_avg_5", 2).alias("cost_adjusted_avg_5_rounded"),
    )
)

metrics_shipments = (
    cleaned
    .select(
        "*",
        F.sum("cost_adjusted_avg_5").over(w4).alias("cumulative_cost_adjusted_avg_5"),
        F.sum("cost_adjusted_avg_5_rounded").over(w5).alias("running_cost_adjusted_avg_5_rounded"),
        F.first("cost_adjusted", ignorenulls=True).over(w6).alias("first_known_cost_adjusted"),
    )
)

ranked = (
    metrics_shipments
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select(
        "*",
        F.last("first_known_cost_adjusted", ignorenulls=True).over(w9).alias("last_known_first_known_cost_adjusted"),
        F.rank().over(w_ordered).alias("order_id_avg_cost_adjusted_rank"),
    )
)
