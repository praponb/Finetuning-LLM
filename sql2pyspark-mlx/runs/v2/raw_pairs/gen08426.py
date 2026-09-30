from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "credit_limit",
        "city",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "segment", "credit_limit", "city")
    .orderBy("credit_limit")
)
