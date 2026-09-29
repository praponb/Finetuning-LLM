from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("id", "status", "order_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("id").desc())
)
