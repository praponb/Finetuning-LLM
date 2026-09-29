from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("amount") > 570)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("discount_bucket"),
    )
)
