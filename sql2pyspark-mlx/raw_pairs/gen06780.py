from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id", "carrier").orderBy("months_shipped_date_to_delivered_date", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-07-31").cast("date"))
    .withColumn("carrier_status_key", F.concat_ws("-", "carrier", "status"))
    .withColumn("months_shipped_date_to_delivered_date", F.round((F.datediff("delivered_date", "shipped_date") / 30), 1))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .filter(F.month("delivered_date").isin([7, 11, 12]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "carrier_status_key",
        F.sum("months_shipped_date_to_delivered_date").over(w_recent).alias("last7_sum_months_shipped_date_to_delivered_date"),
        F.min("months_shipped_date_to_delivered_date").over(w_recent).alias("last7_min_months_shipped_date_to_delivered_date"),
        F.avg("months_shipped_date_to_delivered_date").over(w_recent).alias("last7_avg_months_shipped_date_to_delivered_date"),
        F.max("months_shipped_date_to_delivered_date").over(w_recent).alias("last7_max_months_shipped_date_to_delivered_date"),
    )
)
