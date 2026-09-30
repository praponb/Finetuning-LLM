from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "segment",
        "signup_date",
        F.row_number().over(w).alias("credit_limit_row"),
        (F.col("credit_limit") - F.lag("credit_limit", 1).over(w2)).alias("credit_limit_change"),
    )
)
