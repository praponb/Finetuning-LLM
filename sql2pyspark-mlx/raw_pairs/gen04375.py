from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-07-18")))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "quantity", "row_num")
)

latest_inventory = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id")
)

result = (
    latest_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
