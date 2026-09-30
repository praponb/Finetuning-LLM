from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status", "store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "status",
        "store_id",
        "order_date",
        F.count("*").over(w).alias("customer_id_count"),
        F.row_number().over(w2).alias("row_num"),
    )
)
