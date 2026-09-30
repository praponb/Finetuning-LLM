from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-03-23")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
