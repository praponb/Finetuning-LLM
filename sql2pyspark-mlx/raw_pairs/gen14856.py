from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("quantity") > 219)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
)
