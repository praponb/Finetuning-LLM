from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")

latest = (
    df_shipments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("cost") >= 82, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
    )
    .orderBy(F.col("cost_tier").desc_nulls_first())
)
