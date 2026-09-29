from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        "discount",
        F.first("amount").over(w).alias("first_amount"),
        F.row_number().over(w2).alias("discount_row"),
    )
)
