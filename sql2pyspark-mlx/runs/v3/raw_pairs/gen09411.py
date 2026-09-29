from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "discount",
        "customer_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc())
)
