from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "order_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
)
