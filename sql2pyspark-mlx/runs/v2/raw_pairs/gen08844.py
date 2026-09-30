from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc())
w2 = Window.partitionBy("country").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "age",
        "country",
        "signup_date",
        "credit_limit",
        F.rank().over(w).alias("rnk"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc(), "id")
)
