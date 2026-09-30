from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        (F.col("credit_limit") - F.lag("credit_limit", 1).over(w)).alias("credit_limit_change"),
    )
)
