from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "customer_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "order_date", "customer_id")
    .orderBy("order_date")
)
