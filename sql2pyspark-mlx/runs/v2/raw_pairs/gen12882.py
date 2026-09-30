from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "shipping_fee",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "status", "amount", "shipping_fee")
    .orderBy(F.col("id").desc())
)
