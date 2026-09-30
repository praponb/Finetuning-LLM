from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy(F.year("signup_date"), "segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        F.first("credit_limit").over(w).alias("first_credit_limit"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
)
