from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 79, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
)
