from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("avg_quantity").desc())

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .withColumn("avg_quantity_rank", F.rank().over(w))
    .orderBy(F.col("avg_quantity").desc())
)
