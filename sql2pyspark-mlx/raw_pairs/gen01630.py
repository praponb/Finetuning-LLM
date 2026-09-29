from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "order_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "carrier", "delivered_date", "order_id")
    .orderBy("id", F.col("delivered_date").desc())
)
