from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date"))
)

result = (
    with_latest
    .groupBy(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        "status",
        "store_id",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count_2"),
    )
    .orderBy(F.col("partner_count_2").desc_nulls_last())
)
