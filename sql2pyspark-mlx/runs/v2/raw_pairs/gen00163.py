from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc())

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store"]))
    .select(
        "id",
        "channel",
        "discount",
        F.percent_rank().over(w).alias("discount_pct_rank"),
    )
    .orderBy("discount")
)
