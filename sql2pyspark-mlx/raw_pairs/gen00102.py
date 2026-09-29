from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-29").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
