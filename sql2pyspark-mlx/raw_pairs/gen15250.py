from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "status",
        "store_id",
        "amount",
        F.ntile(4).over(w).alias("amount_ntile"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
