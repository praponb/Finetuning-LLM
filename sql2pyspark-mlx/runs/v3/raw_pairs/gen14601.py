from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "amount",
        "channel",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "amount", "channel")
    .orderBy(F.col("id").desc())
)
