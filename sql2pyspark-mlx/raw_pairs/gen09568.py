from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("carrier", "status").orderBy("cost", "id")
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w4 = Window.orderBy("next_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy(F.col("row_seq").desc(), "id")
w6 = Window.partitionBy("carrier").orderBy("row_seq", "id")

ranked = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "carrier", "shipped_date", "cost", "status")
)

ranked_shipments = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        F.lag("cost").over(w3).alias("prev_cost"),
        F.lead("cost").over(w3).alias("next_cost"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .withColumn("cumulative_next_cost", F.sum("next_cost").over(w4))
    .withColumn("row_seq_bucket", F.ntile(10).over(w5))
    .withColumn("next_prev_cost", F.lead("prev_cost", 1).over(w6))
)

result = (
    with_windows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .cube(F.col("d.status").alias("status"), F.col("d.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.row_seq")).alias("total_row_seq"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("channel").desc())
)
