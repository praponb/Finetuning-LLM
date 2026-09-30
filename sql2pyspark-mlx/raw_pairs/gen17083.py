from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") > 18)
    .select(
        "id",
        "store_id",
        "discount",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.row_number().over(w).alias("row_seq"),
    )
)
