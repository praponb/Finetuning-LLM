from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
)
