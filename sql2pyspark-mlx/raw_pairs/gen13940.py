from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("delivered_date").desc_nulls_last(), "id")

latest = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("WEEK", "shipped_date").alias("week_start"), "status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
)
