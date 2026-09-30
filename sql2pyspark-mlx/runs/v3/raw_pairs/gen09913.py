from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "amount",
        "customer_id",
        "shipping_fee",
        F.max("amount").over(w).alias("store_id_max_amount"),
        F.row_number().over(w2).alias("row_num"),
    )
)
