from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "*",
        F.ntile(5).over(w).alias("amount_bucket"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
