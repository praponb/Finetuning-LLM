from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "order_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "carrier", "delivered_date", "order_id")
    .orderBy(F.col("delivered_date").desc())
)
