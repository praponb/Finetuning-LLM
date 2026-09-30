from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("rn_2"),
    )
)
