from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("customer_id")

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 30, 38]))
    .select(
        "id",
        "store_id",
        "amount",
        "customer_id",
        F.row_number().over(w).alias("row_num"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
