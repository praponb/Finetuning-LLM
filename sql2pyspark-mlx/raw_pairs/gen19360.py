from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id", "city").orderBy("last4_count_quantity", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-08-16").cast("date"))
    .select(
        "*",
        F.when(F.col("quantity") >= 242, "High").when(F.col("quantity") >= 58, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

scored = (
    valid
    .filter(F.month("last_restock").isin([6, 8, 10]))
    .withColumn("last4_sum_quantity", F.sum("quantity").over(w_grp))
    .withColumn("last4_max_quantity", F.max("quantity").over(w_grp))
    .withColumn("last4_count_quantity", F.count("quantity").over(w_grp))
)

joined = (
    scored
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.city"))
)

joined_inventory = (
    joined
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.store_name"))
)

scored_inventory = (
    joined_inventory
    .select(
        "*",
        F.lead("last4_max_quantity").over(w2).alias("next_last4_max_quantity"),
        F.lag("quantity").over(w2).alias("prev_quantity"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    scored_inventory
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.city").alias("store_city"),
        F.col("d.region").alias("region"),
        F.col("src.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.seq_no")), 2).alias("avg_seq_no"),
    )
)
