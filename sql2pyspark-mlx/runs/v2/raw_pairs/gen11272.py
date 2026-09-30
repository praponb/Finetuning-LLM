from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select(
        "id",
        "order_date",
        "customer_id",
        "amount",
        F.row_number().over(w).alias("order_date_row"),
        F.max("amount").over(w2).alias("customer_id_max_amount"),
    )
)
