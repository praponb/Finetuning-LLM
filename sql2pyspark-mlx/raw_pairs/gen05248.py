from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .cube("order_id")
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count_if(F.col("carrier") == "UPS").alias("ups_events"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
