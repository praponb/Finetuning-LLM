from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("amount") < 1390)
    .select(
        "id",
        "status",
        "amount",
        "method",
        F.first("method").over(w).alias("first_method"),
    )
)
