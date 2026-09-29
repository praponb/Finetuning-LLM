from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "customer_id",
        F.rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("store_id").desc(), "id")
)
