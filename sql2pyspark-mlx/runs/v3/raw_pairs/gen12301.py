from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "discount",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "store_id", "shipping_fee", "discount")
)
