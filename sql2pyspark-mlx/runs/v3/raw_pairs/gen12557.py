from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")

result = (
    df_shipments
    .select("id", "order_id", "cost", "status", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "order_id", "cost", "status")
)
