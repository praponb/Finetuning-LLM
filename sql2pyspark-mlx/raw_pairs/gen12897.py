from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

result = (
    df_orders
    .filter(~F.col("status").isin(["RETURNED", "PENDING", "COMPLETED"]))
    .select(
        "id",
        "status",
        "shipping_fee",
        "amount",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .orderBy("status")
)
