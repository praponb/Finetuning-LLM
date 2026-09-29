from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("shipped_date"), "order_id").orderBy("delivered_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("carrier")
w_part = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_shipments
    .withColumn("prev_cost", F.lag("cost", 1).over(w_running))
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w2))
    .withColumn("cost_avg_6", F.avg("cost").over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w_part))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "cost_avg_6",
        "first_known_carrier",
        F.when(F.col("carrier").rlike("^(FedEx|UPS)$"), "OTHER_ZONE").when(F.col("carrier").rlike("^(DHL|Kerry)$"), "EXTENDED").otherwise("GROUP_B").alias("carrier_zone"),
    )
    .orderBy(F.col("carrier_zone").desc(), F.col("carrier").desc())
)
