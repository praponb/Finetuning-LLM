from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        "order_date",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "customer_id", "discount", "order_date")
)
