from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc_nulls_last())
w_running = Window.orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "*",
        F.percent_rank().over(w).alias("shipping_fee_percent_rank"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
