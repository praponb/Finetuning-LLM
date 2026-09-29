from pyspark.sql import functions as F

src = df_inventory.filter((F.col("product_id") == 5) | F.col("store_id").isin([1, 3, 4]))

agg = (
    src
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = agg
