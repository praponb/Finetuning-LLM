from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date")
w2 = Window.orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.col("order_date").between("2023-10-29", "2024-04-01"))
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        F.rank().over(w).alias("order_date_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
