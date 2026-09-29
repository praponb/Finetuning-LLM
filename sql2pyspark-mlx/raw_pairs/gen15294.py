from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store", "mobile"]))
    .select("id", "channel", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "channel", "amount")
    .orderBy("channel")
)
