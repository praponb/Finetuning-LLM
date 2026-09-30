from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

deduped = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "product_id")
)

result = (
    deduped
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count("*").alias("num_rows_2"),
    )
    .orderBy(F.col("order_id").desc_nulls_last())
)
