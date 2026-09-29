from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

deduped = (
    df_order_items
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("unit_price") >= 440, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
    )
)
