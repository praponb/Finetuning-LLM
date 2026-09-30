from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "amount",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
)
