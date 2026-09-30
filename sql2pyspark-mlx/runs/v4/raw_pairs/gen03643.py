from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

result = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
