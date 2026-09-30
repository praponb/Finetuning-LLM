from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "COMPLETED", "RETURNED"]))
    .select("id", "status", "discount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
)
