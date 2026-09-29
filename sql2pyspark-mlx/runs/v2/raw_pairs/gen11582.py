from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select("id", "status", "cost", "order_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "status", "cost", "order_id")
    .orderBy("cost")
)
