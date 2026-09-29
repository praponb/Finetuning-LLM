from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2025)
    .select(
        "id",
        "order_id",
        "cost",
        "delivered_date",
        "shipped_date",
        F.row_number().over(w).alias("rn"),
        F.first("shipped_date").over(w2).alias("first_shipped_date"),
    )
    .orderBy(F.col("order_id").desc())
)
