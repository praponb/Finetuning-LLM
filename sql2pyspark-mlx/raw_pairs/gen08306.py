from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("delivered_date").desc_nulls_first(), "id")

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(F.when(F.col("cost") >= 104, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("*").alias("row_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
)
