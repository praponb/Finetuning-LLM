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
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
    )
    .orderBy(F.col("running_credit_limit").desc(), F.col("credit_limit").desc())
)
