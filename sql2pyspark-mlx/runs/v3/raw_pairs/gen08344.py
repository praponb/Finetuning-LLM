from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
)
