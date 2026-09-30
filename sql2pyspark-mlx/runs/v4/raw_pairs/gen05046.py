from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "amount",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
)
