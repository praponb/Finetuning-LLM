from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last())

ranked = (
    df_customers
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "segment")
)

result = (
    ranked
    .groupBy("segment")
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("row_count_2").desc_nulls_last())
)
