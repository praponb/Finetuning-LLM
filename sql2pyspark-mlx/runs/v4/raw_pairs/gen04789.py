from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("delivered_date").asc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .withColumn("cost_tier", F.when(F.col("cost") >= 90, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "shipped_date"))
    .groupingSets(
        [["cost_tier", "activity_month"], ["cost_tier"], []],
        "cost_tier",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .orderBy(F.col("cost_tier").desc())
)
