from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        F.row_number().over(w).alias("credit_limit_row"),
    )
)
