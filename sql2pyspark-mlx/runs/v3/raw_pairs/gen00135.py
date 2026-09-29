from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        "is_active",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "country", "credit_limit", "is_active")
    .orderBy("country")
)
