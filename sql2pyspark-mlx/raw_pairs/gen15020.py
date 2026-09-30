from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date")).orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "credit_limit",
        F.row_number().over(w).alias("credit_limit_row"),
    )
    .orderBy(F.col("id").desc())
)
