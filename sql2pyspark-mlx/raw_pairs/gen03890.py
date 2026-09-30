from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quantity_tier").orderBy(F.col("acme_count").desc())

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.brand"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity") >= 218, "HIGH").otherwise("LOW").alias("quantity_tier"),
        "store_id",
    )
    .agg(
        F.max(F.when(F.col("brand") == "Globex", F.col("cost"))).alias("globex_max_cost"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
    )
    .withColumn("acme_count_rank", F.rank().over(w))
    .orderBy(F.col("globex_cost").asc_nulls_first())
)
