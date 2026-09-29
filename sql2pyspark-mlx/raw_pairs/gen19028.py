from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"))
)

result = (
    with_latest
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount"))).alias("web_total_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("discount"))).alias("returned_max_discount"),
    )
)
