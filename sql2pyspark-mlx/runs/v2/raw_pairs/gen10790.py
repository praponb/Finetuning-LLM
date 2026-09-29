from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")

result = (
    df_orders
    .select("id", "channel", "discount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "channel", "discount")
    .orderBy("channel")
)
