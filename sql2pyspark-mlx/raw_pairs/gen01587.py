from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc())

latest = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "delivered_date")
)

result = (
    latest
    .groupBy(
        "order_id",
        F.date_trunc("quarter", "delivered_date").alias("quarter_start"),
        F.date_trunc("month", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
