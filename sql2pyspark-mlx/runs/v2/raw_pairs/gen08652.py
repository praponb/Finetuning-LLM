from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "order_date",
        "store_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "status", "order_date", "store_id")
    .orderBy(F.col("id").desc(), F.col("order_date").desc())
)
