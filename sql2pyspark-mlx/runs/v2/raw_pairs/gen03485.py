from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 22, 24]))
    .select(
        "id",
        "customer_id",
        "status",
        "order_date",
        F.row_number().over(w).alias("order_date_row"),
    )
)
