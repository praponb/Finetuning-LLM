from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "order_id",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "carrier", "shipped_date", "order_id")
    .orderBy("carrier")
)
