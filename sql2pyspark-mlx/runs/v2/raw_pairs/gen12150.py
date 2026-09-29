from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "order_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "carrier", "delivered_date", "order_id")
)
