from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        F.dense_rank().over(w).alias("order_date_dense_rank"),
    )
)
