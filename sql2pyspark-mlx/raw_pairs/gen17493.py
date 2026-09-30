from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("amount_row"),
    )
)
