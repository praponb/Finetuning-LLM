from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "order_date", "status")
    .orderBy("order_date", F.col("id").desc())
)
