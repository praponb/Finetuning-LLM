from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w_part = Window.partitionBy("channel").orderBy("discount", "id")

result = (
    df_orders
    .select(
        "*",
        F.ntile(3).over(w).alias("amount_bucket"),
        F.lead("amount", 1, 0).over(w_part).alias("next_amount"),
    )
)
