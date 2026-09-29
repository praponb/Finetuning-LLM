from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sqft").desc_nulls_last())
w2 = Window.orderBy("sqft", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("store_id").orderBy("days_since_last_restock", "id")
w4 = Window.partitionBy("product_id", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region", "store_id").orderBy(F.col("sqft").asc_nulls_last())
w6 = Window.partitionBy("product_id").orderBy("sqft", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-30").cast("date"))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

enriched = (
    filtered
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"), F.col("s.sqft"), F.col("s.region"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched_inventory = (
    ranked
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.store_name"))
)

result = (
    enriched_inventory
    .withColumn("quantity_avg_3_prior", F.avg("quantity").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w4))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w5))
    .withColumn("sqft_bucket", F.ntile(3).over(w6))
)
