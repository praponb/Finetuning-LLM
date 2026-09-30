from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(3).over(w_part).alias("amount_bucket"),
        F.stddev_samp("amount").over(w2).alias("order_id_stddev_samp_amount"),
    )
)
