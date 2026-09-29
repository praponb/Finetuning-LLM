from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("cost")
w2 = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").asc_nulls_first())

result = (
    df_shipments
    .select(
        "*",
        F.percent_rank().over(w_hist).alias("cost_percent_rank"),
        F.rank().over(w2).alias("cost_rank"),
    )
)
