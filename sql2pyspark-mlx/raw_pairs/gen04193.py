from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("discount").over(w).alias("next_discount"),
        F.row_number().over(w).alias("row_seq"),
    )
)
