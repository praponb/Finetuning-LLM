from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        F.lead("discount", 1).over(w).alias("next_discount"),
    )
)
