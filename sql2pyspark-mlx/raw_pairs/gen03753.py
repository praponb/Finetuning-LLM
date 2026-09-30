from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("last_restock") < "2023-06-01")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
