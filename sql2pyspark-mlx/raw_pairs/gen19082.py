from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .filter(F.col("credit_limit") < 10700)
    .select(
        "id",
        "city",
        "credit_limit",
        "signup_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("credit_limit").desc())
)
