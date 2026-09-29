from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("delivered_date", "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w5 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .withColumn("order_id_max_cost", F.max("cost").over(w))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w5))
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"), "carrier")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.avg(F.when(F.col("status") == "RETURNED", F.col("last_known_cost"))).alias("returned_avg_last_known_cost"),
        F.max(F.when(F.col("carrier") == "UPS", F.col("seq_no"))).alias("ups_max_seq_no"),
        F.round(F.avg("last_known_cost"), 2).alias("avg_last_known_cost"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.round(F.avg("last_known_cost"), 2).alias("avg_last_known_cost_2"),
    )
    .orderBy(F.col("carrier").desc())
)
