from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 24]))
    .select(
        "id",
        "status",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
)
