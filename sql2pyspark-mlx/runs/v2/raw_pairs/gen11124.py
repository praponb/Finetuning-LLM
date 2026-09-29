from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.row_number().over(w2).alias("row_num"),
    )
)
