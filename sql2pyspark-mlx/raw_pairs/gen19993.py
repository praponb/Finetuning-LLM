from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("quantity_percent_rank_tier").orderBy(F.col("distinct_store_ids").desc())

enriched = (
    df_inventory
    .filter(F.col("quantity").between(96, 196))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_6"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity_percent_rank") >= 0.78, "PREMIUM").otherwise("STANDARD").alias("quantity_percent_rank_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("last_known_quantity", [0.25, 0.5, 0.75]).alias("last_known_quantity_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("last_known_quantity").alias("avg_last_known_quantity"),
        F.percentile_approx("quantity_avg_6", [0.25, 0.5, 0.75]).alias("quantity_avg_6_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .withColumn("distinct_store_ids_dense_rank", F.dense_rank().over(w4))
)
