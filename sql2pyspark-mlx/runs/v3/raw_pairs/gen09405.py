from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit").between(6900, 14900))
    .select(
        "id",
        "segment",
        "credit_limit",
        "email",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
)
