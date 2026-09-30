from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select("id", "status", "amount", "discount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .select("id", "status", "amount", "discount")
)
