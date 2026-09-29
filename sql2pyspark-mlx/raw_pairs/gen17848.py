from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("cost").desc_nulls_last())

ranked = (
    df_shipments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("WEEK", "shipped_date").alias("week_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_rows"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("cost_per_returned"),
    )
)
