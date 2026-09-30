from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("metric_value_tier").orderBy(F.col("shipping_fee_events").desc())

unpivoted = (
    df_orders
    .select("id", "channel", "amount", "shipping_fee", "discount")
    .unpivot(
        ids=["id", "channel"],
        values=["amount", "shipping_fee", "discount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("metric_value") >= 86, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
        "metric_name",
    )
    .agg(
        F.round(F.avg("metric_value"), 2).alias("avg_metric_value"),
        F.count_if(F.col("metric_name") == "shipping_fee").alias("shipping_fee_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("shipping_fee_events_rank", F.rank().over(w))
    .orderBy(F.col("metric_name").desc_nulls_first())
)
