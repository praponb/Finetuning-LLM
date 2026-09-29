from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "order_id", "delivered_date", "status")
    .orderBy(F.col("order_id").desc())
)
