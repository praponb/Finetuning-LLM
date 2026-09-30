from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date").between("2024-12-01", "2025-08-31"))
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "order_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "customer_id", "shipping_fee", "order_date")
)
