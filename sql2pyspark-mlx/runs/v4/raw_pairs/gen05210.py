from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(~F.col("channel").isin(["partner", "mobile", "web"]))
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "store_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "customer_id", "shipping_fee", "store_id")
)
