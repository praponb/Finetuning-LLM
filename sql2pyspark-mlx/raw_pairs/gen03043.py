from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_first())
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_avg_5").desc_nulls_first(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("cost_avg_5").desc(), "id")

latest = (
    df_shipments
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("order_id").isin([7, 17, 36, 39]))
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.avg("cost").over(w3).alias("cost_avg_5"),
    )
)

latest_shipments = (
    enriched
    .withColumn("dedup_rank_2", F.row_number().over(w4))
    .filter(F.col("dedup_rank_2") == 1)
)

ranked = (
    latest_shipments
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .cube(
        "status",
        F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("row_seq"))).alias("fedex_total_row_seq"),
    )
)
