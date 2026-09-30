from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("cost_tier").orderBy(F.col("avg_cost").desc())

ranked = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("cost") >= 99, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .withColumn("avg_cost_rank", F.rank().over(w2))
)
