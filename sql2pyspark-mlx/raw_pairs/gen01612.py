from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.ntile(2).over(w).alias("quartile"),
        F.rank().over(w2).alias("credit_limit_rank"),
    )
)
