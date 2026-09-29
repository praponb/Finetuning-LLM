from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        "status",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "customer_id", "discount", "status")
)
